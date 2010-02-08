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

package org.elasticsearch.bootstrap;

import com.google.inject.CreationException;
import com.google.inject.spi.Message;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.env.Environment;
import org.elasticsearch.jmx.JmxService;
import org.elasticsearch.server.Server;
import org.elasticsearch.server.ServerBuilder;
import org.elasticsearch.server.internal.InternalSettingsPerparer;
import org.elasticsearch.util.Classes;
import org.elasticsearch.util.Tuple;
import org.elasticsearch.util.jline.ANSI;
import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.logging.log4j.LogConfigurator;
import org.elasticsearch.util.settings.Settings;
import org.slf4j.Logger;

import java.io.File;
import java.util.Set;

import static com.google.common.collect.Sets.*;
import static jline.ANSIBuffer.ANSICodes.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
public class Bootstrap {

    private Server server;

    private void setup(boolean addShutdownHook) throws Exception {
        Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(EMPTY_SETTINGS, true);

        try {
            Classes.getDefaultClassLoader().loadClass("org.apache.log4j.Logger");
            LogConfigurator.configure(tuple.v1());
        } catch (ClassNotFoundException e) {
            // no log4j
        } catch (NoClassDefFoundError e) {
            // no log4j
        } catch (Exception e) {
            System.err.println("Failed to configure logging...");
            e.printStackTrace();
        }

        if (tuple.v1().get(JmxService.SettingsConstants.CREATE_CONNECTOR) == null) {
            // automatically create the connector if we are bootstrapping
            Settings updated = settingsBuilder().putAll(tuple.v1()).putBoolean(JmxService.SettingsConstants.CREATE_CONNECTOR, true).build();
            tuple = new Tuple<Settings, Environment>(updated, tuple.v2());
        }

        ServerBuilder serverBuilder = ServerBuilder.serverBuilder().settings(tuple.v1()).loadConfigSettings(false);
        server = serverBuilder.build();
        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override public void run() {
                    server.close();
                }
            });
        }
    }

    /**
     * hook for JSVC
     */
    public void init(String[] args) throws Exception {
        setup(true);
    }

    /**
     * hook for JSVC
     */
    public void start() {
        server.start();
    }

    /**
     * hook for JSVC
     */
    public void stop() {
        server.stop();
    }


    /**
     * hook for JSVC
     */
    public void destroy() {
        server.close();
    }


    public static void main(String[] args) {
        Bootstrap bootstrap = new Bootstrap();
        String pidFile = System.getProperty("es-pidfile");

        boolean foreground = System.getProperty("es-foreground") != null;

        String stage = "Initialization";
        try {
            if (!foreground) {
                Loggers.disableConsoleLogging();
                System.out.close();
            }
            bootstrap.setup(true);

            if (pidFile != null) {
                new File(pidFile).deleteOnExit();
            }

            stage = "Startup";
            bootstrap.start();

            if (!foreground) {
                System.err.close();
            }
        } catch (Throwable e) {
            Logger logger = Loggers.getLogger(Bootstrap.class);
            if (bootstrap.server != null) {
                logger = Loggers.getLogger(Bootstrap.class, bootstrap.server.settings().get("name"));
            }
            StringBuilder errorMessage = new StringBuilder("{").append(Version.full()).append("}: ");
            try {
                if (ANSI.isEnabled()) {
                    errorMessage.append(attrib(ANSI.Code.FG_RED)).append(stage).append(" Failed ...").append(attrib(ANSI.Code.OFF)).append("\n");
                } else {
                    errorMessage.append(stage).append(" Failed ...\n");
                }
            } catch (Throwable t) {
                errorMessage.append(stage).append(" Failed ...\n");
            }
            if (e instanceof CreationException) {
                CreationException createException = (CreationException) e;
                Set<String> seenMessages = newHashSet();
                int counter = 1;
                for (Message message : createException.getErrorMessages()) {
                    String detailedMessage;
                    if (message.getCause() == null) {
                        detailedMessage = message.getMessage();
                    } else {
                        detailedMessage = ExceptionsHelper.detailedMessage(message.getCause(), true, 0);
                    }
                    if (detailedMessage == null) {
                        detailedMessage = message.getMessage();
                    }
                    if (seenMessages.contains(detailedMessage)) {
                        continue;
                    }
                    seenMessages.add(detailedMessage);
                    errorMessage.append("").append(counter++).append(") ").append(detailedMessage);
                }
            } else {
                errorMessage.append("- ").append(ExceptionsHelper.detailedMessage(e, true, 0));
            }
            if (foreground) {
                logger.error(errorMessage.toString());
            } else {
                System.err.println(errorMessage);
                System.err.flush();
            }
            Loggers.disableConsoleLogging();
            if (logger.isDebugEnabled()) {
                logger.debug("Exception", e);
            }
            System.exit(3);
        }
    }
}
