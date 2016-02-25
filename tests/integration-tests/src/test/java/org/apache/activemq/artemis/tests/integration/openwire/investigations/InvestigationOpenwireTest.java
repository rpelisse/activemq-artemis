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

package org.apache.activemq.artemis.tests.integration.openwire.investigations;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.junit.Assert;
import org.junit.Test;

public class InvestigationOpenwireTest extends BasicOpenWireTest {

   @Test
   public void testSimple() throws Exception {
      try {

         Connection connection = factory.createConnection();
         //      Thread.sleep(5000);

         Collection<Session> sessions = new LinkedList<>();

         for (int i = 0; i < 10; i++) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            sessions.add(session);
         }

         connection.close();

         System.err.println("Done!!!");
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testAutoAck() throws Exception {
      try {

         Connection connection = factory.createConnection();
         //      Thread.sleep(5000);

         Collection<Session> sessions = new LinkedList<>();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createTextMessage("test"));

         Assert.assertNull(consumer.receive(100));
         connection.start();

         TextMessage message = (TextMessage)consumer.receive(5000);

         Assert.assertNotNull(message);


         connection.close();

         System.err.println("Done!!!");
      }
      catch (Throwable e) {
         e.printStackTrace();
      }
   }



   @Test
   public void testRollback() throws Exception {
      try {

         Connection connection = factory.createConnection();
         //      Thread.sleep(5000);

         Collection<Session> sessions = new LinkedList<>();

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createTextMessage("test"));
         connection.start();
         Assert.assertNull(consumer.receive(1000));
         session.rollback();
         producer.send(session.createTextMessage("test2"));
         Assert.assertNull(consumer.receive(1000));
         session.commit();
         TextMessage msg = (TextMessage)consumer.receive(1000);


         Assert.assertNotNull(msg);
         Assert.assertEquals("test2", msg.getText());

         connection.close();

         System.err.println("Done!!!");
      }
      catch (Throwable e) {
         e.printStackTrace();
      }
   }


   @Test
   public void testClientACK() throws Exception {
      try {

         Connection connection = factory.createConnection();
         //      Thread.sleep(5000);

         Collection<Session> sessions = new LinkedList<>();

         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createTextMessage("test"));

         Assert.assertNull(consumer.receive(100));
         connection.start();

         TextMessage message = (TextMessage)consumer.receive(5000);

         Assert.assertNotNull(message);

         message.acknowledge();


         connection.close();

         System.err.println("Done!!!");
      }
      catch (Throwable e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testXASimple() throws Exception {
      try {

         XAConnection connection = xaFactory.createXAConnection();
         //      Thread.sleep(5000);

         Collection<Session> sessions = new LinkedList<>();

         for (int i = 0; i < 10; i++) {
            XASession session = connection.createXASession();
            session.getXAResource().start(newXID(), XAResource.TMNOFLAGS);
            sessions.add(session);
         }

         connection.close();

         System.err.println("Done!!!");
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }
}
