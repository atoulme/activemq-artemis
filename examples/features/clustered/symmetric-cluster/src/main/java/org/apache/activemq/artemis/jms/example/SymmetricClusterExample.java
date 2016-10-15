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
package org.apache.activemq.artemis.jms.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.tests.integration.mqtt.imported.FuseMQTTClientProvider;
import org.apache.activemq.artemis.tests.integration.mqtt.imported.MQTTClientProvider;

/**
 * This example demonstrates a cluster of three nodes set up in a symmetric topology - i.e. each
 * node is connected to every other node in the cluster. Also each node, has it's own backup node.
 * <p>
 * This is probably the most obvious clustering topology and the one most people will be familiar
 * with from using clustering in an app server, where every node has pretty much identical
 * configuration to every other node.
 * <p>
 * By clustering nodes symmetrically, ActiveMQ Artemis can give the impression of clustered queues, topics
 * and durable subscriptions.
 * <p>
 * In this example we send some messages to a distributed queue and topic and kill all the live
 * servers at different times, and verify that they transparently fail over onto their backup
 * servers.
 * <p>
 * Please see the readme.html file for more information.
 */
public class SymmetricClusterExample {

   private static final int NUM_MESSAGES = 10;

   public static void main(final String[] args) throws Exception {

      try {
         // Step 1 - We instantiate a connection factory directly, specifying the UDP address and port for discovering
         // the list of servers in the cluster.
         // We could use JNDI to look-up a connection factory, but we'd need to know the JNDI server host and port for
         // the
         // specific server to do that, and that server might not be available at the time. By creating the
         // connection factory directly we avoid having to worry about a JNDI look-up.
         // In an app server environment you could use HA-JNDI to lookup from the clustered JNDI servers without
         // having to know about a specific one.
         UDPBroadcastEndpointFactory udpCfg = new UDPBroadcastEndpointFactory();
         udpCfg.setGroupAddress("231.7.7.7").setGroupPort(9876);
         DiscoveryGroupConfiguration groupConfiguration = new DiscoveryGroupConfiguration();
         groupConfiguration.setBroadcastEndpointFactory(udpCfg);

         // We give a little while for each server to broadcast its whereabouts to the client
         Thread.sleep(2000);

         // MQTT now.
         final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
         subscriptionProvider.connect("tcp://localhost:" + 1885);

         subscriptionProvider.subscribe("mqtt/#", 0);

         final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES * 2);

         Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
               for (int i = 0; i < NUM_MESSAGES; i++) {
                  try {
                     byte[] payload = subscriptionProvider.receive(10000);
                     if (payload == null) {
                        throw new Exception("Nothing received after 10s");
                     }
                     System.err.println("Received message on pattern subscription " + i + ":" + new String(payload));
                     latch.countDown();
                  } catch (Exception e) {
                     e.printStackTrace();
                     break;
                  }
               }
            }
         });
         thread.start();

         final MQTTClientProvider publishProvider = getMQTTClientProvider();
         publishProvider.connect("tcp://localhost:" + 1883);
         // final MQTTClientProvider publishProvider2 = getMQTTClientProvider();
         // publishProvider2.connect("tcp://localhost:" + 1887);

         for (int i = 0; i < NUM_MESSAGES; i++) {
            String payload = "Message " + i;
            System.err.println("Sending " + payload);
            publishProvider.publish("mqtt/bar" + i, payload.getBytes(), 0);
            // System.err.println("Sending again " + payload);
            // publishProvider2.publish("mqtt/bar" + i, payload.getBytes(), 1);
         }

         latch.await(10, TimeUnit.SECONDS);
         subscriptionProvider.disconnect();
         publishProvider.disconnect();
         // publishProvider2.disconnect();
         System.err.println("==== Messages remaining: " + latch.getCount() + " ====");

      } catch(Exception e) {
         e.printStackTrace();
      } finally {
      }
   }

   public static MQTTClientProvider getMQTTClientProvider() {
      return new FuseMQTTClientProvider();
   }
}
