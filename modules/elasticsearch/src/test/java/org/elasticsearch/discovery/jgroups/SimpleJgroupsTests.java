/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.jgroups;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleJgroupsTests {

    static {
        System.setProperty("jgroups.logging.log_factory_class", JgroupsCustomLogFactory.class.getName());
    }

    private JChannel channel1;

    private JChannel channel2;

    @BeforeMethod public void setupChannels() throws ChannelException {
        channel1 = new JChannel("udp.xml");
        channel1.connect("test");

        channel2 = new JChannel("udp.xml");
        channel2.connect("test");
    }

    @AfterMethod public void closeChannels() {
        channel1.close();
        channel2.close();
    }

    @Test public void testUdpJgroups() throws Exception {
        channel1.send(new Message(null, channel1.getAddress(), "test"));
    }
}
