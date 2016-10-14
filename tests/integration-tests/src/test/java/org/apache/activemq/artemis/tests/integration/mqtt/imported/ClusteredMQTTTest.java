/**
 *
 */
package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ClusteredMQTTTest extends MQTTTestSupport {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final int SERVER2_MQTT_PORT = 10883;

   private ActiveMQServer server2;

   @Override
   protected boolean isStartServer() {
      return false;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server2 = createServerForMQTT();
      addCoreConnector(6555);
      addMQTTConnector(SERVER2_MQTT_PORT);
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(999999999);
      addressSettings.setAutoCreateJmsQueues(true);

      server2.getAddressSettingsRepository().addMatch("#", addressSettings);

      TransportConfiguration connectorFrom = createTransportConfiguration(true, false, generateParams(0, true));
      server.getConfiguration().getConnectorConfigurations().put(connectorFrom.getName(), connectorFrom);

      List<String> pairs = new ArrayList<>();
      TransportConfiguration serverTotc = createTransportConfiguration(true, false, generateParams(1, true));
      server.getConfiguration().getConnectorConfigurations().put(serverTotc.getName(), serverTotc);
      pairs.add(serverTotc.getName());
      Configuration config = server.getConfiguration();
      ClusterConnectionConfiguration clusterConf =
               createClusterConfig("cluster", "cluster", MessageLoadBalancingType.ON_DEMAND, 1, connectorFrom, pairs);

      config.getClusterConfigurations().add(clusterConf);

      server.start();
      server2.start();
      server.waitForActivation(10, TimeUnit.SECONDS);
      server2.waitForActivation(10, TimeUnit.SECONDS);

   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();
      if (server2.isStarted()) {
         server2.stop();
      }
   }

   private static final int NUM_MESSAGES = 20;

   @Test
   public void testPubSubOverCluster() throws Exception {
      final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
      subscriptionProvider.connect("tcp://localhost:" + SERVER2_MQTT_PORT);

      subscriptionProvider.subscribe("mqtt/#", 1);

      final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);

      Thread thread = new Thread(new Runnable() {
         @Override
         public void run() {
            for (int i = 0; i < NUM_MESSAGES; i++) {
               try {
                  byte[] payload = subscriptionProvider.receive(10000);
                  System.err.println("Received message on pattern subscription " + i + ":" + new String(payload));
                  assertNotNull("Should get a message", payload);
                  latch.countDown();
               } catch (Exception e) {
                  log.error("Error", e);
                  e.printStackTrace();
                  break;
               }
            }
         }
      });
      thread.start();

      final MQTTClientProvider publishProvider = getMQTTClientProvider();
      publishProvider.connect("tcp://localhost:" + port);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         String payload = "Message " + i;
         System.err.println("Sending " + payload);
         publishProvider.publish("mqtt/bar" + i, payload.getBytes(), 1);
      }

      latch.await(10, TimeUnit.SECONDS);
      assertEquals(0, latch.getCount());
      subscriptionProvider.disconnect();
      publishProvider.disconnect();
   }

   private ClusterConnectionConfiguration createClusterConfig(final String name, final String address,
                                                              final MessageLoadBalancingType messageLoadBalancingType,
                                                              final int maxHops, TransportConfiguration connectorFrom,
                                                              List<String> pairs) {
      return new ClusterConnectionConfiguration().setName(name)
                                                 .setAddress(address)
                                                 .setConnectorName(connectorFrom.getName())
                                                 .setRetryInterval(250)
                                                 .setMessageLoadBalancingType(messageLoadBalancingType)
                                                 .setMaxHops(maxHops)
                                                 .setConfirmationWindowSize(1024)
                                                 .setStaticConnectors(pairs);
   }

}
