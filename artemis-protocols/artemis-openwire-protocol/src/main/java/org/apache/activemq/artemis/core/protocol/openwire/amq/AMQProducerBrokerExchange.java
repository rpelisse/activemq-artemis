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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.state.ProducerState;

public class AMQProducerBrokerExchange {

   private AMQConnectionContext connectionContext;
   private ProducerState producerState;
   private boolean mutable = true;
   private final FlowControlInfo flowControlInfo = new FlowControlInfo();

   public AMQProducerBrokerExchange() {
   }

   public AMQProducerBrokerExchange copy() {
      AMQProducerBrokerExchange rc = new AMQProducerBrokerExchange();
      rc.connectionContext = connectionContext.copy();
      rc.producerState = producerState;
      rc.mutable = mutable;
      return rc;
   }

   /**
    * @return the connectionContext
    */
   public AMQConnectionContext getConnectionContext() {
      return this.connectionContext;
   }

   /**
    * @param connectionContext the connectionContext to set
    */
   public void setConnectionContext(AMQConnectionContext connectionContext) {
      this.connectionContext = connectionContext;
   }

   /**
    * @param mutable the mutable to set
    */
   public void setMutable(boolean mutable) {
      this.mutable = mutable;
   }

   /**
    * @return the producerState
    */
   public ProducerState getProducerState() {
      return this.producerState;
   }

   /**
    * @param producerState the producerState to set
    */
   public void setProducerState(ProducerState producerState) {
      this.producerState = producerState;
   }

   public void setLastStoredSequenceId(long l) {
   }

   public void blockingOnFlowControl(boolean blockingOnFlowControl) {
      flowControlInfo.setBlockingOnFlowControl(blockingOnFlowControl);
   }

   public static class FlowControlInfo {

      private AtomicBoolean blockingOnFlowControl = new AtomicBoolean();
      private AtomicLong totalSends = new AtomicLong();
      private AtomicLong sendsBlocked = new AtomicLong();
      private AtomicLong totalTimeBlocked = new AtomicLong();

      public void setBlockingOnFlowControl(boolean blockingOnFlowControl) {
         this.blockingOnFlowControl.set(blockingOnFlowControl);
         if (blockingOnFlowControl) {
            incrementSendBlocked();
         }
      }

      public void incrementSendBlocked() {
         this.sendsBlocked.incrementAndGet();
      }

      public void reset() {
         blockingOnFlowControl.set(false);
         totalSends.set(0);
         sendsBlocked.set(0);
         totalTimeBlocked.set(0);

      }
   }

}
