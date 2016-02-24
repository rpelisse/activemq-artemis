/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.openwire.impl;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;

public class OpenWireServerCallback implements SessionCallback {

   @Override
   public boolean hasCredits(ServerConsumer consumerID) {
      return false;
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {

   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {

   }

   @Override
   public int sendMessage(ServerMessage message, ServerConsumer consumerID, int deliveryCount) {
      return 0;
   }

   @Override
   public int sendLargeMessage(ServerMessage message, ServerConsumer consumerID, long bodySize, int deliveryCount) {
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumerID,
                                           byte[] body,
                                           boolean continues,
                                           boolean requiresResponse) {
      return 0;
   }

   @Override
   public void closed() {

   }

   @Override
   public void disconnect(ServerConsumer consumerId, String queueName) {

   }

   @Override
   public boolean isWritable(ReadyListener callback) {
      return false;
   }
}
