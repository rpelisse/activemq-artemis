/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.openwire;

import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSSecurityException;
import javax.jms.ResourceAllocationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManager;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireUtil;
import org.apache.activemq.artemis.core.protocol.openwire.SendingResult;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQCompositeConsumerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConnectionContext;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQProducerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSession;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSingleConsumerBrokerExchange;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.spi.core.protocol.AbstractRemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.transport.TransmitCallback;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Represents an activemq connection.
 * ToDo: extends AbstractRemotingConnection
 */
public class OpenWireConnection extends AbstractRemotingConnection implements SecurityAuth {

   private final OpenWireProtocolManager protocolManager;

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<>();

   private boolean destroyed = false;

   private final Object sendLock = new Object();

   private final Acceptor acceptorUsed;

   private final OpenWireFormat wireFormat;

   private AMQConnectionContext context;

   private Throwable stopError = null;

   private final AtomicBoolean stopping = new AtomicBoolean(false);

   private final ReentrantReadWriteLock serviceLock = new ReentrantReadWriteLock();

   protected final List<Command> dispatchQueue = new LinkedList<>();

   private boolean inServiceException;

   private final AtomicBoolean asyncException = new AtomicBoolean(false);

   private final Map<ConsumerId, AMQConsumerBrokerExchange> consumerExchanges = new HashMap<>();
   private final Map<ProducerId, AMQProducerBrokerExchange> producerExchanges = new HashMap<>();

   private ConnectionState state;

   private final Set<ActiveMQDestination> tempQueues = new ConcurrentHashSet<>();

   private Map<TransactionId, TransactionInfo> txMap = new ConcurrentHashMap<>();

   private volatile AMQSession advisorySession;

   private String defaultSocketURIString;

   public OpenWireConnection(Acceptor acceptorUsed,
                             Connection connection,
                             Executor executor,
                             OpenWireProtocolManager openWireProtocolManager,
                             OpenWireFormat wf) {
      super(connection, executor);
      this.protocolManager = openWireProtocolManager;
      this.acceptorUsed = acceptorUsed;
      this.wireFormat = wf;
      this.defaultSocketURIString = connection.getLocalAddress();
   }

   // SecurityAuth implementation
   @Override
   public String getUsername() {
      ConnectionInfo info = getConnectionInfo();
      if (info == null) {
         return null;
      }
      return info.getUserName();
   }

   // SecurityAuth implementation
   @Override
   public RemotingConnection getRemotingConnection() {
      return this;
   }

   // SecurityAuth implementation
   @Override
   public String getPassword() {
      ConnectionInfo info = getConnectionInfo();
      if (info == null) {
         return null;
      }
      return info.getPassword();
   }

   private ConnectionInfo getConnectionInfo() {
      if (state == null) {
         return null;
      }
      ConnectionInfo info = state.getInfo();
      if (info == null) {
         return null;
      }
      return info;
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      super.bufferReceived(connectionID, buffer);
      try {

         // TODO-NOW: set OperationContext

         Command command = (Command) wireFormat.unmarshal(buffer);

         boolean responseRequired = command.isResponseRequired();
         int commandId = command.getCommandId();
         // the connection handles pings, negotiations directly.
         // and delegate all other commands to manager.
         if (command.getClass() == KeepAliveInfo.class) {
            KeepAliveInfo info = (KeepAliveInfo) command;
            info.setResponseRequired(false);
            // if we don't respond to KeepAlive commands then the client will think the server is dead and timeout
            // for some reason KeepAliveInfo.isResponseRequired() is always false
            protocolManager.sendReply(this, info);
         }
         else {
            Response response = null;

            try {
               setLastCommand(command);
               response = command.visit(new CommandProcessor());
            }
            catch (Exception e) {
               if (responseRequired) {
                  response = new ExceptionResponse(e);
               }
            }
            finally {
               setLastCommand(null);
            }

            if (response instanceof ExceptionResponse) {
               if (!responseRequired) {
                  Throwable cause = ((ExceptionResponse) response).getException();
                  serviceException(cause);
                  response = null;
               }
            }

            if (responseRequired) {
               if (response == null) {
                  response = new Response();
                  response.setCorrelationId(command.getCommandId());
               }
            }

            // The context may have been flagged so that the response is not
            // sent.
            if (context != null) {
               if (context.isDontSendReponse()) {
                  context.setDontSendReponse(false);
                  response = null;
               }
            }

            // TODO-NOW: response through operation-context

            if (response != null && !protocolManager.isStopping()) {
               response.setCorrelationId(commandId);
               dispatchSync(response);
            }

         }
      }
      catch (IOException e) {

         // TODO-NOW: send errors
         ActiveMQServerLogger.LOGGER.error("error decoding", e);
      }
      catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.error("error decoding", t);
      }
   }

   private void setLastCommand(Command command) {
      if (context != null) {
         context.setLastCommand(command);
      }
   }

   @Override
   public void destroy() {
      fail(null, null);
   }

   @Override
   public boolean isClient() {
      return false;
   }

   @Override
   public boolean isDestroyed() {
      return destroyed;
   }

   @Override
   public void disconnect(boolean criticalError) {
      this.disconnect(null, null, criticalError);
   }

   @Override
   public boolean checkDataReceived() {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   @Override
   public void flush() {
   }

   private void callFailureListeners(final ActiveMQException me) {
      final List<FailureListener> listenersClone = new ArrayList<>(failureListeners);

      for (final FailureListener listener : listenersClone) {
         try {
            listener.connectionFailed(me, false);
         }
         catch (final Throwable t) {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   // throw a WireFormatInfo to the peer
   public void init() {
      WireFormatInfo info = wireFormat.getPreferedWireFormatInfo();
      protocolManager.send(this, info);
   }

   public ConnectionState getState() {
      return state;
   }

   public void physicalSend(Command command) throws IOException {
      try {
         ByteSequence bytes = wireFormat.marshal(command);
         ActiveMQBuffer buffer = OpenWireUtil.toActiveMQBuffer(bytes);
         synchronized (sendLock) {
            getTransportConnection().write(buffer, false, false);
         }
      }
      catch (IOException e) {
         throw e;
      }
      catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.error("error sending", t);
      }

   }

   public void dispatchAsync(Command message) {
      if (!stopping.get()) {
         dispatchSync(message);
      }
      else {
         if (message.isMessageDispatch()) {
            MessageDispatch md = (MessageDispatch) message;
            TransmitCallback sub = md.getTransmitCallback();
            protocolManager.postProcessDispatch(md);
            if (sub != null) {
               sub.onFailure();
            }
         }
      }
   }

   public void dispatchSync(Command message) {
      try {
         processDispatch(message);
      }
      catch (IOException e) {
         serviceExceptionAsync(e);
      }
   }

   public void serviceExceptionAsync(final IOException e) {
      if (asyncException.compareAndSet(false, true)) {
         // TODO: Why this is not through an executor?
         new Thread("Async Exception Handler") {
            @Override
            public void run() {
               serviceException(e);
            }
         }.start();
      }
   }

   public void serviceException(Throwable e) {
      // are we a transport exception such as not being able to dispatch
      // synchronously to a transport
      if (e instanceof IOException) {
         serviceTransportException((IOException) e);
      }
      else if (!stopping.get() && !inServiceException) {
         inServiceException = true;
         try {
            ConnectionError ce = new ConnectionError();
            ce.setException(e);
            dispatchAsync(ce);
         }
         finally {
            inServiceException = false;
         }
      }
   }

   public void serviceTransportException(IOException e) {
      /*
       * deal with it later BrokerService bService =
       * connector.getBrokerService(); if (bService.isShutdownOnSlaveFailure())
       * { if (brokerInfo != null) { if (brokerInfo.isSlaveBroker()) {
       * LOG.error("Slave has exception: {} shutting down master now.",
       * e.getMessage(), e); try { doStop(); bService.stop(); } catch (Exception
       * ex) { LOG.warn("Failed to stop the master", ex); } } } } if
       * (!stopping.get() && !pendingStop) { transportException.set(e); if
       * (TRANSPORTLOG.isDebugEnabled()) { TRANSPORTLOG.debug(this + " failed: "
       * + e, e); } else if (TRANSPORTLOG.isWarnEnabled() && !expected(e)) {
       * TRANSPORTLOG.warn(this + " failed: " + e); } stopAsync(); }
       */
   }

   protected void dispatch(Command command) throws IOException {
      this.physicalSend(command);
   }

   protected void processDispatch(Command command) throws IOException {
      MessageDispatch messageDispatch = (MessageDispatch) (command.isMessageDispatch() ? command : null);
      try {
         if (!stopping.get()) {
            if (messageDispatch != null) {
               protocolManager.preProcessDispatch(messageDispatch);
            }
            dispatch(command);
         }
      }
      catch (IOException e) {
         if (messageDispatch != null) {
            TransmitCallback sub = messageDispatch.getTransmitCallback();
            protocolManager.postProcessDispatch(messageDispatch);
            if (sub != null) {
               sub.onFailure();
            }
            messageDispatch = null;
            throw e;
         }
      }
      finally {
         if (messageDispatch != null) {
            TransmitCallback sub = messageDispatch.getTransmitCallback();
            protocolManager.postProcessDispatch(messageDispatch);
            if (sub != null) {
               sub.onSuccess();
            }
         }
      }
   }

   public void addConsumerBrokerExchange(ConsumerId id,
                                         AMQSession amqSession,
                                         Map<ActiveMQDestination, AMQConsumer> consumerMap) {
      AMQConsumerBrokerExchange result = consumerExchanges.get(id);
      if (result == null) {
         if (consumerMap.size() == 1) {
            result = new AMQSingleConsumerBrokerExchange(amqSession, consumerMap.values().iterator().next());
         }
         else {
            result = new AMQCompositeConsumerBrokerExchange(amqSession, consumerMap);
         }
         synchronized (consumerExchanges) {
            result.setConnectionContext(context);
            SessionState ss = state.getSessionState(id.getParentId());
            if (ss != null) {
               ConsumerState cs = ss.getConsumerState(id);
               if (cs != null) {
                  ConsumerInfo info = cs.getInfo();
                  if (info != null) {
                     if (info.getDestination() != null && info.getDestination().isPattern()) {
                        result.setWildcard(true);
                     }
                  }
               }
            }
            consumerExchanges.put(id, result);
         }
      }
   }

   private AMQProducerBrokerExchange getProducerBrokerExchange(ProducerId id) throws IOException {
      AMQProducerBrokerExchange result = producerExchanges.get(id);
      if (result == null) {
         synchronized (producerExchanges) {
            result = new AMQProducerBrokerExchange();
            result.setConnectionContext(context);
            //todo implement reconnect https://issues.apache.org/jira/browse/ARTEMIS-194
            //todo: this used to check for  && this.acceptorUsed.isAuditNetworkProducers()
            if (context.isReconnect() || (context.isNetworkConnection())) {
               // once implemented ARTEMIS-194, we need to set the storedSequenceID here somehow
               // We have different semantics on Artemis Journal, but we could adapt something for this
               // TBD during the implemetnation of ARTEMIS-194
               result.setLastStoredSequenceId(0);
            }
            SessionState ss = state.getSessionState(id.getParentId());
            if (ss != null) {
               result.setProducerState(ss.getProducerState(id));
               ProducerState producerState = ss.getProducerState(id);
               if (producerState != null && producerState.getInfo() != null) {
                  ProducerInfo info = producerState.getInfo();
                  result.setMutable(info.getDestination() == null || info.getDestination().isComposite());
               }
            }
            producerExchanges.put(id, result);
         }
      }
      return result;
   }

   private void removeConsumerBrokerExchange(ConsumerId id) {
      synchronized (consumerExchanges) {
         consumerExchanges.remove(id);
      }
   }

   public void deliverMessage(MessageDispatch dispatch) {
      Message m = dispatch.getMessage();
      if (m != null) {
         long endTime = System.currentTimeMillis();
         m.setBrokerOutTime(endTime);
      }

      protocolManager.send(this, dispatch);
   }

   public WireFormat getMarshaller() {
      return this.wireFormat;
   }

   public void registerTempQueue(ActiveMQDestination queue) {
      tempQueues.add(queue);
   }

   private void shutdown(boolean fail) {
      if (fail) {
         transportConnection.forceClose();
      }
      else {
         transportConnection.close();
      }
   }

   private void disconnect(ActiveMQException me, String reason, boolean fail) {

      if (context == null || destroyed) {
         return;
      }
      // Don't allow things to be added to the connection state while we
      // are shutting down.
      // is it necessary? even, do we need state at all?
      state.shutdown();

      // Then call the listeners
      // this should closes underlying sessions
      callFailureListeners(me);

      // this should clean up temp dests
      synchronized (sendLock) {
         callClosingListeners();
      }

      destroyed = true;

      //before closing transport, send the last response if any
      Command command = context.getLastCommand();
      if (command != null && command.isResponseRequired()) {
         Response lastResponse = new Response();
         lastResponse.setCorrelationId(command.getCommandId());
         dispatchSync(lastResponse);
         context.setDontSendReponse(true);
      }
   }

   @Override
   public void disconnect(String reason, boolean fail) {
      this.disconnect(null, reason, fail);
   }

   @Override
   public void fail(ActiveMQException me, String message) {
      if (me != null) {
         ActiveMQServerLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      }
      try {
         protocolManager.removeConnection(this, this.getConnectionInfo(), me);
      }
      catch (InvalidClientIDException e) {
         ActiveMQServerLogger.LOGGER.warn("Couldn't close connection because invalid clientID", e);
      }
      shutdown(true);
   }

   public void setAdvisorySession(AMQSession amqSession) {
      this.advisorySession = amqSession;
   }

   public AMQSession getAdvisorySession() {
      return this.advisorySession;
   }

   public AMQConnectionContext getContext() {
      return this.context;
   }

   public String getDefaultSocketURIString() {
      return defaultSocketURIString;
   }

   public void updateClient(ConnectionControl control) {
      //      if (!destroyed && context.isFaultTolerant()) {
      if (protocolManager.isUpdateClusterClients()) {
         dispatchAsync(control);
      }
      //      }
   }

   public AMQConnectionContext initContext(ConnectionInfo info) {
      WireFormatInfo wireFormatInfo = wireFormat.getPreferedWireFormatInfo();
      // Older clients should have been defaulting this field to true.. but
      // they were not.
      if (wireFormatInfo != null && wireFormatInfo.getVersion() <= 2) {
         info.setClientMaster(true);
      }

      state = new ConnectionState(info);

      context = new AMQConnectionContext();

      state.reset(info);

      // Setup the context.
      String clientId = info.getClientId();
      context.setBroker(protocolManager);
      context.setClientId(clientId);
      context.setClientMaster(info.isClientMaster());
      context.setConnection(this);
      context.setConnectionId(info.getConnectionId());
      // for now we pass the manager as the connector and see what happens
      // it should be related to activemq's Acceptor
      context.setFaultTolerant(info.isFaultTolerant());
      context.setUserName(info.getUserName());
      context.setWireFormatInfo(wireFormatInfo);
      context.setReconnect(info.isFailoverReconnect());
      context.setConnectionState(state);
      if (info.getClientIp() == null) {
         info.setClientIp(getRemoteAddress());
      }

      return context;
   }

   //raise the refCount of context
   public void reconnect(AMQConnectionContext existingContext, ConnectionInfo info) {
      this.context = existingContext;
      WireFormatInfo wireFormatInfo = wireFormat.getPreferedWireFormatInfo();
      // Older clients should have been defaulting this field to true.. but
      // they were not.
      if (wireFormatInfo != null && wireFormatInfo.getVersion() <= 2) {
         info.setClientMaster(true);
      }
      if (info.getClientIp() == null) {
         info.setClientIp(getRemoteAddress());
      }

      state = new ConnectionState(info);
      state.reset(info);

      context.setConnection(this);
      context.setConnectionState(state);
      context.setClientMaster(info.isClientMaster());
      context.setFaultTolerant(info.isFaultTolerant());
      context.setReconnect(true);
      context.incRefCount();
   }

   // This will listen for commands throught the protocolmanager
   class CommandProcessor implements CommandVisitor {

      @Override
      public Response processAddConnection(ConnectionInfo info) throws Exception {
         //let protoclmanager handle connection add/remove
         try {
            protocolManager.addConnection(OpenWireConnection.this, info);
         }
         catch (Exception e) {
            Response resp = new ExceptionResponse(e);
            return resp;
         }
         if (info.isManageable() && protocolManager.isUpdateClusterClients()) {
            // send ConnectionCommand
            ConnectionControl command = protocolManager.newConnectionControl();
            command.setFaultTolerant(protocolManager.isFaultTolerantConfiguration());
            if (info.isFailoverReconnect()) {
               command.setRebalanceConnection(false);
            }
            dispatchAsync(command);
         }
         return null;

      }

      @Override
      public Response processAddProducer(ProducerInfo info) throws Exception {
         Response resp = null;
         try {
            protocolManager.addProducer(OpenWireConnection.this, info);
         }
         catch (Exception e) {
            if (e instanceof ActiveMQSecurityException) {
               resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
            }
            else if (e instanceof ActiveMQNonExistentQueueException) {
               resp = new ExceptionResponse(new InvalidDestinationException(e.getMessage()));
            }
            else {
               resp = new ExceptionResponse(e);
            }
         }
         return resp;
      }

      @Override
      public Response processAddConsumer(ConsumerInfo info) {
         Response resp = null;
         try {
            protocolManager.addConsumer(OpenWireConnection.this, info);
         }
         catch (Exception e) {
            e.printStackTrace();
            if (e instanceof ActiveMQSecurityException) {
               resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
            }
            else {
               resp = new ExceptionResponse(e);
            }
         }
         return resp;
      }

      @Override
      public Response processRemoveDestination(DestinationInfo info) throws Exception {
         ActiveMQDestination dest = info.getDestination();
         protocolManager.removeDestination(OpenWireConnection.this, dest);
         return null;
      }

      @Override
      public Response processRemoveProducer(ProducerId id) throws Exception {
         protocolManager.removeProducer(id);
         return null;
      }

      @Override
      public Response processRemoveSession(SessionId id, long lastDeliveredSequenceId) throws Exception {
         SessionState session = state.getSessionState(id);
         if (session == null) {
            throw new IllegalStateException("Cannot remove session that had not been registered: " + id);
         }
         // Don't let new consumers or producers get added while we are closing
         // this down.
         session.shutdown();
         // Cascade the connection stop producers.
         // we don't stop consumer because in core
         // closing the session will do the job
         for (ProducerId producerId : session.getProducerIds()) {
            try {
               processRemoveProducer(producerId);
            }
            catch (Throwable e) {
               // LOG.warn("Failed to remove producer: {}", producerId, e);
            }
         }
         state.removeSession(id);
         protocolManager.removeSession(context, session.getInfo());
         return null;
      }

      @Override
      public Response processRemoveSubscription(RemoveSubscriptionInfo subInfo) throws Exception {
         protocolManager.removeSubscription(subInfo);
         return null;
      }

      @Override
      public Response processRollbackTransaction(TransactionInfo info) throws Exception {
         protocolManager.rollbackTransaction(info);
         TransactionId txId = info.getTransactionId();
         txMap.remove(txId);
         return null;
      }

      @Override
      public Response processShutdown(ShutdownInfo info) throws Exception {
         OpenWireConnection.this.shutdown(false);
         return null;
      }

      @Override
      public Response processWireFormat(WireFormatInfo command) throws Exception {
         wireFormat.renegotiateWireFormat(command);
         //throw back a brokerInfo here
         protocolManager.sendBrokerInfo(OpenWireConnection.this);
         return null;
      }

      @Override
      public Response processAddDestination(DestinationInfo dest) throws Exception {
         Response resp = null;
         try {
            protocolManager.addDestination(OpenWireConnection.this, dest);
         }
         catch (Exception e) {
            if (e instanceof ActiveMQSecurityException) {
               resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
            }
            else {
               resp = new ExceptionResponse(e);
            }
         }
         return resp;
      }

      @Override
      public Response processAddSession(SessionInfo info) throws Exception {
         // Avoid replaying dup commands
         if (!state.getSessionIds().contains(info.getSessionId())) {
            protocolManager.addSession(OpenWireConnection.this, info);
            try {
               state.addSession(info);
            }
            catch (IllegalStateException e) {
               e.printStackTrace();
               protocolManager.removeSession(context, info);
            }
         }
         return null;
      }

      @Override
      public Response processBeginTransaction(TransactionInfo info) throws Exception {
         TransactionId txId = info.getTransactionId();

         if (!txMap.containsKey(txId)) {
            txMap.put(txId, info);
         }
         return null;
      }

      @Override
      public Response processBrokerInfo(BrokerInfo arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception {
         protocolManager.commitTransactionOnePhase(info);
         TransactionId txId = info.getTransactionId();
         txMap.remove(txId);

         return null;
      }

      @Override
      public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception {
         protocolManager.commitTransactionTwoPhase(info);
         TransactionId txId = info.getTransactionId();
         txMap.remove(txId);

         return null;
      }

      @Override
      public Response processConnectionControl(ConnectionControl connectionControl) throws Exception {
         //activemq5 keeps a var to remember only the faultTolerant flag
         //this can be sent over a reconnected transport as the first command
         //before restoring the connection.
         return null;
      }

      @Override
      public Response processConnectionError(ConnectionError arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processConsumerControl(ConsumerControl consumerControl) throws Exception {
         //amq5 clients send this command to restore prefetchSize
         //after successful reconnect
         try {
            protocolManager.updateConsumer(OpenWireConnection.this, consumerControl);
         }
         catch (Exception e) {
            //log error
         }
         return null;
      }

      @Override
      public Response processControlCommand(ControlCommand arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processEndTransaction(TransactionInfo info) throws Exception {
         protocolManager.endTransaction(info);
         TransactionId txId = info.getTransactionId();

         if (!txMap.containsKey(txId)) {
            txMap.put(txId, info);
         }
         return null;
      }

      @Override
      public Response processFlush(FlushCommand arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processForgetTransaction(TransactionInfo info) throws Exception {
         TransactionId txId = info.getTransactionId();
         txMap.remove(txId);

         protocolManager.forgetTransaction(info.getTransactionId());
         return null;
      }

      @Override
      public Response processKeepAlive(KeepAliveInfo arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processMessage(Message messageSend) {
         Response resp = null;
         try {
            ProducerId producerId = messageSend.getProducerId();
            AMQProducerBrokerExchange producerExchange = getProducerBrokerExchange(producerId);
            final AMQConnectionContext pcontext = producerExchange.getConnectionContext();
            final ProducerInfo producerInfo = producerExchange.getProducerState().getInfo();
            boolean sendProducerAck = !messageSend.isResponseRequired() && producerInfo.getWindowSize() > 0 && !pcontext.isInRecoveryMode();

            AMQSession session = protocolManager.getSession(producerId.getParentId());

            // TODO: canDispatch is always returning true;
            if (producerExchange.canDispatch(messageSend)) {
               SendingResult result = session.send(producerExchange, messageSend, sendProducerAck);
               if (result.isBlockNextSend()) {
                  if (!context.isNetworkConnection() && result.isSendFailIfNoSpace()) {
                     // TODO see logging
                     throw new ResourceAllocationException("Usage Manager Memory Limit reached. Stopping producer (" + producerId + ") to prevent flooding " + result.getBlockingAddress() + "." + " See http://activemq.apache.org/producer-flow-control.html for more info");
                  }

                  if (producerInfo.getWindowSize() > 0 || messageSend.isResponseRequired()) {
                     //in that case don't send the response
                     //this will force the client to wait until
                     //the response is got.
                     context.setDontSendReponse(true);
                  }
                  else {
                     //hang the connection until the space is available
                     session.blockingWaitForSpace(producerExchange, result);
                  }
               }
               else if (sendProducerAck) {
                  ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), messageSend.getSize());
                  OpenWireConnection.this.dispatchAsync(ack);
               }
            }
         }
         catch (Throwable e) {
            if (e instanceof ActiveMQSecurityException) {
               resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
            }
            else {
               resp = new ExceptionResponse(e);
            }
         }
         return resp;
      }

      @Override
      public Response processMessageAck(MessageAck ack) throws Exception {
         AMQConsumerBrokerExchange consumerBrokerExchange = consumerExchanges.get(ack.getConsumerId());
         consumerBrokerExchange.acknowledge(ack);
         return null;
      }

      @Override
      public Response processMessageDispatch(MessageDispatch arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processMessageDispatchNotification(MessageDispatchNotification arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processMessagePull(MessagePull arg0) throws Exception {
         AMQConsumerBrokerExchange amqConsumerBrokerExchange = consumerExchanges.get(arg0.getConsumerId());
         if (amqConsumerBrokerExchange == null) {
            throw new IllegalStateException("Consumer does not exist");
         }
         amqConsumerBrokerExchange.processMessagePull(arg0);
         return null;
      }

      @Override
      public Response processPrepareTransaction(TransactionInfo info) throws Exception {
         protocolManager.prepareTransaction(info);
         return null;
      }

      @Override
      public Response processProducerAck(ProducerAck arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processRecoverTransactions(TransactionInfo info) throws Exception {
         Set<SessionId> sIds = state.getSessionIds();
         TransactionId[] recovered = protocolManager.recoverTransactions(sIds);
         return new DataArrayResponse(recovered);
      }

      @Override
      public Response processRemoveConnection(ConnectionId id, long lastDeliveredSequenceId) throws Exception {
         //we let protocol manager to handle connection add/remove
         try {
            protocolManager.removeConnection(OpenWireConnection.this, state.getInfo(), null);
         }
         catch (Throwable e) {
            // log
         }
         return null;
      }

      @Override
      public Response processRemoveConsumer(ConsumerId id, long lastDeliveredSequenceId) throws Exception {
         SessionId sessionId = id.getParentId();
         SessionState ss = state.getSessionState(sessionId);
         if (ss == null) {
            throw new IllegalStateException("Cannot remove a consumer from a session that had not been registered: " + sessionId);
         }
         ConsumerState consumerState = ss.removeConsumer(id);
         if (consumerState == null) {
            throw new IllegalStateException("Cannot remove a consumer that had not been registered: " + id);
         }
         ConsumerInfo info = consumerState.getInfo();
         info.setLastDeliveredSequenceId(lastDeliveredSequenceId);

         AMQConsumerBrokerExchange consumerBrokerExchange = consumerExchanges.get(id);

         consumerBrokerExchange.removeConsumer();

         removeConsumerBrokerExchange(id);

         return null;
      }

   }

}
