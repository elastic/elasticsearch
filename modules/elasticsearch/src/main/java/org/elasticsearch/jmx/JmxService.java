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

package org.elasticsearch.jmx;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.PortsRange;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author kimchy (shay.banon)
 */
public class JmxService {

    public static class SettingsConstants {

        public static final String CREATE_CONNECTOR = "jmx.create_connector";
    }

    // we use {jmx.port} without prefix of $ since we don't want it to be resolved as a setting property

    public static final String JMXRMI_URI_PATTERN = "service:jmx:rmi:///jndi/rmi://:{jmx.port}/jmxrmi";

    public static final String JMXRMI_PUBLISH_URI_PATTERN = "service:jmx:rmi:///jndi/rmi://{jmx.host}:{jmx.port}/jmxrmi";

    private final ESLogger logger;

    private final Settings settings;

    private final String jmxDomain;

    private String serviceUrl;

    private String publishUrl;

    private final MBeanServer mBeanServer;

    private JMXConnectorServer connectorServer;

    private final CopyOnWriteArrayList<ResourceDMBean> constructionMBeans = new CopyOnWriteArrayList<ResourceDMBean>();

    private final CopyOnWriteArrayList<ResourceDMBean> registeredMBeans = new CopyOnWriteArrayList<ResourceDMBean>();

    private String nodeDescription;

    private volatile boolean started = false;

    public JmxService(ESLogger logger, final Settings settings) {
        this.logger = logger;
        this.settings = settings;

        this.jmxDomain = settings.get("jmx.domain", "org.elasticsearch");

        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    public String serviceUrl() {
        return this.serviceUrl;
    }

    public String publishUrl() {
        return this.publishUrl;
    }

    public void connectAndRegister(String nodeDescription, final NetworkService networkService) {
        if (started) {
            return;
        }
        started = true;
        this.nodeDescription = nodeDescription;
        if (settings.getAsBoolean(SettingsConstants.CREATE_CONNECTOR, false)) {
            // we are going to create the connector, set the GC interval to a large value
            try {
                if (System.getProperty("sun.rmi.dgc.client.gcInterval") == null)
                    System.setProperty("sun.rmi.dgc.client.gcInterval", "36000000");
                if (System.getProperty("sun.rmi.dgc.server.gcInterval") == null)
                    System.setProperty("sun.rmi.dgc.server.gcInterval", "36000000");
            } catch (Exception secExc) {
                logger.warn("Failed to set sun.rmi.dgc.xxx system properties", secExc);
            }

            final String port = settings.get("jmx.port", "9400-9500");

            PortsRange portsRange = new PortsRange(port);
            final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
            boolean success = portsRange.iterate(new PortsRange.PortCallback() {
                @Override public boolean onPortNumber(int portNumber) {
                    try {
                        LocateRegistry.createRegistry(portNumber);
                        serviceUrl = settings.get("jmx.service_url", JMXRMI_URI_PATTERN).replace("{jmx.port}", Integer.toString(portNumber));
                        // Create the JMX service URL.
                        JMXServiceURL url = new JMXServiceURL(serviceUrl);
                        // Create the connector server now.
                        connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url, settings.getAsMap(), mBeanServer);
                        connectorServer.start();

                        // create the publish url
                        String publishHost = networkService.resolvePublishHostAddress(settings.get("jmx.publish_host")).getHostAddress();
                        publishUrl = settings.get("jmx.publish_url", JMXRMI_PUBLISH_URI_PATTERN).replace("{jmx.port}", Integer.toString(portNumber)).replace("{jmx.host}", publishHost);
                    } catch (Exception e) {
                        lastException.set(e);
                        return false;
                    }
                    return true;
                }
            });
            if (!success) {
                throw new JmxConnectorCreationException("Failed to bind to [" + port + "]", lastException.get());
            }
            logger.info("bound_address {{}}, publish_address {{}}", serviceUrl, publishUrl);
        }

        for (ResourceDMBean resource : constructionMBeans) {
            register(resource);
        }
    }

    public void registerMBean(Object instance) {
        ResourceDMBean resourceDMBean = new ResourceDMBean(instance, logger);
        if (!resourceDMBean.isManagedResource()) {
            return;
        }
        if (!started) {
            constructionMBeans.add(resourceDMBean);
            return;
        }
        register(resourceDMBean);
    }

    public void unregisterGroup(String groupName) {
        for (ResourceDMBean resource : registeredMBeans) {
            if (!groupName.equals(resource.getGroupName())) {
                continue;
            }

            registeredMBeans.remove(resource);

            String resourceName = resource.getFullObjectName();
            try {
                ObjectName objectName = new ObjectName(getObjectName(resourceName));
                if (mBeanServer.isRegistered(objectName)) {
                    mBeanServer.unregisterMBean(objectName);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Unregistered " + objectName);
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to unregister " + resource.getFullObjectName());
            }
        }
    }


    public void close() {
        if (!started) {
            return;
        }
        started = false;
        // unregister mbeans
        for (ResourceDMBean resource : registeredMBeans) {
            String resourceName = resource.getFullObjectName();
            try {
                ObjectName objectName = new ObjectName(getObjectName(resourceName));
                if (mBeanServer.isRegistered(objectName)) {
                    mBeanServer.unregisterMBean(objectName);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Unregistered " + objectName);
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to unregister " + resource.getFullObjectName());
            }
        }
        if (connectorServer != null) {
            try {
                connectorServer.stop();
            } catch (IOException e) {
                logger.debug("Failed to close connector", e);
            }
        }
    }

    private void register(ResourceDMBean resourceDMBean) throws JmxRegistrationException {
        try {
            String resourceName = resourceDMBean.getFullObjectName();
            ObjectName objectName = new ObjectName(getObjectName(resourceName));
            if (!mBeanServer.isRegistered(objectName)) {
                try {
                    mBeanServer.registerMBean(resourceDMBean, objectName);
                    registeredMBeans.add(resourceDMBean);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Registered " + resourceDMBean + " under " + objectName);
                    }
                } catch (InstanceAlreadyExistsException e) {
                    //this might happen if multiple instances are trying to concurrently register same objectName
                    logger.debug("Could not register object with name:" + objectName + "(" + e.getMessage() + ")");
                }
            } else {
                logger.debug("Could not register object with name: " + objectName + ", already registered");
            }
        } catch (Exception e) {
            logger.warn("Could not register object with name: " + resourceDMBean.getFullObjectName() + "(" + e.getMessage() + ")");
        }
    }

    private String getObjectName(String resourceName) {
        return getObjectName(jmxDomain, resourceName);
    }

    private String getObjectName(String jmxDomain, String resourceName) {
        return jmxDomain + ":" + resourceName;
    }
}
