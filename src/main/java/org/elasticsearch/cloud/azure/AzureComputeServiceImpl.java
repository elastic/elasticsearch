/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.cloud.azure;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.settings.SettingsFilter;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class AzureComputeServiceImpl extends AbstractLifecycleComponent<AzureComputeServiceImpl>
    implements AzureComputeService {

    static final class Azure {
        private static final String ENDPOINT = "https://management.core.windows.net/";
        private static final String VERSION = "2013-03-01";
    }

    private SSLSocketFactory socketFactory;
    private final String keystore;
    private final String password;
    private final String subscription_id;
    private final String service_name;
    private final String port_name;

    @Inject
    public AzureComputeServiceImpl(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        settingsFilter.addFilter(new AzureSettingsFilter());

        // Creating socketFactory
        subscription_id = componentSettings.get(Fields.SUBSCRIPTION_ID, settings.get("cloud.azure." + Fields.SUBSCRIPTION_ID));
        service_name = componentSettings.get(Fields.SERVICE_NAME, settings.get("cloud.azure." + Fields.SERVICE_NAME));
        keystore = componentSettings.get(Fields.KEYSTORE, settings.get("cloud.azure." + Fields.KEYSTORE));
        password = componentSettings.get(Fields.PASSWORD, settings.get("cloud.azure." + Fields.PASSWORD));
        port_name = componentSettings.get(Fields.PORT_NAME, settings.get("cloud.azure." + Fields.PORT_NAME, "elasticsearch"));

        // Check that we have all needed properties
        try {
            checkProperty(Fields.SUBSCRIPTION_ID, subscription_id);
            checkProperty(Fields.SERVICE_NAME, service_name);
            checkProperty(Fields.KEYSTORE, keystore);
            checkProperty(Fields.PASSWORD, password);
            socketFactory = getSocketFactory(keystore, password);

            if (logger.isTraceEnabled()) logger.trace("creating new Azure client for [{}], [{}], [{}], [{}]",
                    subscription_id, service_name, port_name);
        } catch (Exception e) {
            // Can not start Azure Client
            logger.error("can not start azure client: {}", e.getMessage());
            socketFactory = null;
        }
    }

    private InputStream getXML(String api) throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException, KeyManagementException {
       String https_url = Azure.ENDPOINT + subscription_id + api;

        URL url = new URL( https_url );
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        con.setSSLSocketFactory( socketFactory );
        con.setRequestProperty("x-ms-version", Azure.VERSION);

        if (logger.isDebugEnabled()) logger.debug("calling azure REST API: {}", api);
        if (logger.isTraceEnabled()) logger.trace("get {} from azure", https_url);

        return con.getInputStream();
    }

    @Override
    public Set<Instance> instances() {
        if (socketFactory == null) {
            // Azure plugin is disabled
            if (logger.isTraceEnabled()) logger.trace("azure plugin is disabled. Returning an empty list of nodes.");
            return new HashSet<Instance>();
        } else {
            try {
                InputStream stream = getXML("/services/hostedservices/" + service_name + "?embed-detail=true");
                Set<Instance> instances = buildInstancesFromXml(stream, port_name);
                if (logger.isTraceEnabled()) logger.trace("get instances from azure: {}", instances);

                return instances;

            } catch (ParserConfigurationException e) {
                logger.warn("can not parse XML response: {}", e.getMessage());
                return new HashSet<Instance>();
            } catch (XPathExpressionException e) {
                logger.warn("can not parse XML response: {}", e.getMessage());
                return new HashSet<Instance>();
            } catch (SAXException e) {
                logger.warn("can not parse XML response: {}", e.getMessage());
                return new HashSet<Instance>();
            } catch (Exception e) {
                logger.warn("can not get list of azure nodes: {}", e.getMessage());
                return new HashSet<Instance>();
            }
        }
    }

    private static String extractValueFromPath(Node node, String path) throws XPathExpressionException {
        XPath xPath =  XPathFactory.newInstance().newXPath();
        Node subnode = (Node) xPath.compile(path).evaluate(node, XPathConstants.NODE);
        return subnode.getFirstChild().getNodeValue();
    }

    public static Set<Instance> buildInstancesFromXml(InputStream inputStream, String port_name) throws ParserConfigurationException, IOException, SAXException, XPathExpressionException {
        Set<Instance> instances = new HashSet<Instance>();

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(inputStream);

        doc.getDocumentElement().normalize();

        XPath xPath =  XPathFactory.newInstance().newXPath();

        // We only fetch Started nodes (TODO: should we start with all nodes whatever the status is?)
        String expression = "/HostedService/Deployments/Deployment/RoleInstanceList/RoleInstance[PowerState='Started']";
        NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(doc, XPathConstants.NODESET);
        for (int i = 0; i < nodeList.getLength(); i++) {
            Instance instance = new Instance();
            Node node = nodeList.item(i);
            instance.setPrivateIp(extractValueFromPath(node, "IpAddress"));
            instance.setName(extractValueFromPath(node, "InstanceName"));
            instance.setStatus(Instance.Status.STARTED);

            // Let's digg into <InstanceEndpoints>
            expression = "InstanceEndpoints/InstanceEndpoint[Name='"+ port_name +"']";
            NodeList endpoints = (NodeList) xPath.compile(expression).evaluate(node, XPathConstants.NODESET);
            for (int j = 0; j < endpoints.getLength(); j++) {
                Node endpoint = endpoints.item(j);
                instance.setPublicIp(extractValueFromPath(endpoint, "Vip"));
                instance.setPublicPort(extractValueFromPath(endpoint, "PublicPort"));
            }

            instances.add(instance);
        }

        return instances;
    }

    private SSLSocketFactory getSocketFactory(String keystore, String password) throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        File pKeyFile = new File(keystore);
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        InputStream keyInput = new FileInputStream(pKeyFile);
        keyStore.load(keyInput, password.toCharArray());
        keyInput.close();
        keyManagerFactory.init(keyStore, password.toCharArray());

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
        return context.getSocketFactory();
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    private void checkProperty(String name, String value) throws ElasticSearchException {
        if (!Strings.hasText(value)) {
            throw new SettingsException("cloud.azure." + name +" is not set or is incorrect.");
        }
    }

}
