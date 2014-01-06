/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.cloud.azure.AzureComputeServiceImpl;
import org.elasticsearch.cloud.azure.Instance;
import org.junit.Assert;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

public class AzureInstanceXmlParserTest {

    private Instance build(String name, String privateIpAddress,
                           String publicIpAddress,
                           String publicPort,
                           Instance.Status status) {
        Instance instance = new Instance();
        instance.setName(name);
        instance.setPrivateIp(privateIpAddress);
        instance.setPublicIp(publicIpAddress);
        instance.setPublicPort(publicPort);
        instance.setStatus(status);
        return instance;
    }

    @Test
    public void testReadXml() throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
        InputStream inputStream = AzureInstanceXmlParserTest.class.getResourceAsStream("/org/elasticsearch/azure/test/services.xml");
        Set<Instance> instances = AzureComputeServiceImpl.buildInstancesFromXml(inputStream, "elasticsearch");

        Set<Instance> expected = new HashSet<Instance>();
        expected.add(build("es-windows2008", "10.53.250.55", null, null, Instance.Status.STARTED));
        expected.add(build("myesnode1", "10.53.218.75", "137.116.213.150", "9300", Instance.Status.STARTED));

        Assert.assertArrayEquals(expected.toArray(), instances.toArray());
    }
}
