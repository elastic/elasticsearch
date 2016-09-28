/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.logging.log4j.core.jmx;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AsyncAppender;
import org.apache.logging.log4j.core.async.AsyncLoggerConfig;
import org.apache.logging.log4j.core.async.AsyncLoggerContext;
import org.apache.logging.log4j.core.async.DaemonThreadFactory;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.selector.ContextSelector;
import org.apache.logging.log4j.core.util.Constants;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.elasticsearch.common.SuppressForbidden;

/**
 * Creates MBeans to instrument various classes in the log4j class hierarchy.
 * <p>
 * All instrumentation for Log4j 2 classes can be disabled by setting system property {@code -Dlog4j2.disable.jmx=true}.
 * </p>
 */
@SuppressForbidden(reason = "copied class to hack around Log4j bug")
public final class Server {

    /**
     * The domain part, or prefix ({@value}) of the {@code ObjectName} of all MBeans that instrument Log4J2 components.
     */
    public static final String DOMAIN = "org.apache.logging.log4j2";
    private static final String PROPERTY_DISABLE_JMX = "log4j2.disable.jmx";
    private static final String PROPERTY_ASYNC_NOTIF = "log4j2.jmx.notify.async";
    private static final String THREAD_NAME_PREFIX = "log4j2.jmx.notif";
    private static final StatusLogger LOGGER = StatusLogger.getLogger();
    static final Executor executor = isJmxDisabled() ? null : createExecutor();

    private Server() {
    }

    /**
     * Returns either a {@code null} Executor (causing JMX notifications to be sent from the caller thread) or a daemon
     * background thread Executor, depending on the value of system property "log4j2.jmx.notify.async". If this
     * property is not set, use a {@code null} Executor for web apps to avoid memory leaks and other issues when the
     * web app is restarted.
     * @see <a href="https://issues.apache.org/jira/browse/LOG4J2-938">LOG4J2-938</a>
     */
    private static ExecutorService createExecutor() {
        final boolean defaultAsync = !Constants.IS_WEB_APP;
        final boolean async = PropertiesUtil.getProperties().getBooleanProperty(PROPERTY_ASYNC_NOTIF, defaultAsync);
        return async ? Executors.newFixedThreadPool(1, new DaemonThreadFactory(THREAD_NAME_PREFIX)) : null;
    }

    /**
     * Either returns the specified name as is, or returns a quoted value containing the specified name with the special
     * characters (comma, equals, colon, quote, asterisk, or question mark) preceded with a backslash.
     *
     * @param name the name to escape so it can be used as a value in an {@link ObjectName}.
     * @return the escaped name
     */
    public static String escape(final String name) {
        final StringBuilder sb = new StringBuilder(name.length() * 2);
        boolean needsQuotes = false;
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            switch (c) {
                case '\\':
                case '*':
                case '?':
                case '\"':
                    // quote, star, question & backslash must be escaped
                    sb.append('\\');
                    needsQuotes = true; // ... and can only appear in quoted value
                    break;
                case ',':
                case '=':
                case ':':
                    // no need to escape these, but value must be quoted
                    needsQuotes = true;
                    break;
                case '\r':
                    // drop \r characters: \\r gives "invalid escape sequence"
                    continue;
                case '\n':
                    // replace \n characters with \\n sequence
                    sb.append("\\n");
                    needsQuotes = true;
                    continue;
            }
            sb.append(c);
        }
        if (needsQuotes) {
            sb.insert(0, '\"');
            sb.append('\"');
        }
        return sb.toString();
    }

    private static boolean isJmxDisabled() {
        return PropertiesUtil.getProperties().getBooleanProperty(PROPERTY_DISABLE_JMX);
    }

    public static void reregisterMBeansAfterReconfigure() {
        // avoid creating Platform MBean Server if JMX disabled
        if (isJmxDisabled()) {
            LOGGER.debug("JMX disabled for log4j2. Not registering MBeans.");
            return;
        }
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        reregisterMBeansAfterReconfigure(mbs);
    }

    public static void reregisterMBeansAfterReconfigure(final MBeanServer mbs) {
        if (isJmxDisabled()) {
            LOGGER.debug("JMX disabled for log4j2. Not registering MBeans.");
            return;
        }

        // now provide instrumentation for the newly configured
        // LoggerConfigs and Appenders
        try {
            final ContextSelector selector = getContextSelector();
            if (selector == null) {
                LOGGER.debug("Could not register MBeans: no ContextSelector found.");
                return;
            }
            LOGGER.trace("Reregistering MBeans after reconfigure. Selector={}", selector);
            final List<LoggerContext> contexts = selector.getLoggerContexts();
            int i = 0;
            for (final LoggerContext ctx : contexts) {
                LOGGER.trace("Reregistering context ({}/{}): '{}' {}", ++i, contexts.size(), ctx.getName(), ctx);
                // first unregister the context and all nested loggers,
                // appenders, statusLogger, contextSelector, ringbuffers...
                unregisterLoggerContext(ctx.getName(), mbs);

                final LoggerContextAdmin mbean = new LoggerContextAdmin(ctx, executor);
                register(mbs, mbean, mbean.getObjectName());

                if (ctx instanceof AsyncLoggerContext) {
                    final RingBufferAdmin rbmbean = ((AsyncLoggerContext) ctx).createRingBufferAdmin();
                    if (rbmbean.getBufferSize() > 0) {
                        // don't register if Disruptor not started (DefaultConfiguration: config not found)
                        register(mbs, rbmbean, rbmbean.getObjectName());
                    }
                }

                // register the status logger and the context selector
                // repeatedly
                // for each known context: if one context is unregistered,
                // these MBeans should still be available for the other
                // contexts.
                registerStatusLogger(ctx.getName(), mbs, executor);
                registerContextSelector(ctx.getName(), selector, mbs, executor);

                registerLoggerConfigs(ctx, mbs, executor);
                registerAppenders(ctx, mbs, executor);
            }
        } catch (final Exception ex) {
            LOGGER.error("Could not register mbeans", ex);
        }
    }

    /**
     * Unregister all log4j MBeans from the platform MBean server.
     */
    public static void unregisterMBeans() {
        if (isJmxDisabled()) {
            LOGGER.debug("JMX disabled for Log4j2. Not unregistering MBeans.");
            return;
        }
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        unregisterMBeans(mbs);
    }

    /**
     * Unregister all log4j MBeans from the specified MBean server.
     *
     * @param mbs the MBean server to unregister from.
     */
    public static void unregisterMBeans(final MBeanServer mbs) {
        unregisterStatusLogger("*", mbs);
        unregisterContextSelector("*", mbs);
        unregisterContexts(mbs);
        unregisterLoggerConfigs("*", mbs);
        unregisterAsyncLoggerRingBufferAdmins("*", mbs);
        unregisterAsyncLoggerConfigRingBufferAdmins("*", mbs);
        unregisterAppenders("*", mbs);
        unregisterAsyncAppenders("*", mbs);
    }

    /**
     * Returns the {@code ContextSelector} of the current {@code Log4jContextFactory}.
     *
     * @return the {@code ContextSelector} of the current {@code Log4jContextFactory}
     */
    private static ContextSelector getContextSelector() {
        final LoggerContextFactory factory = LogManager.getFactory();
        if (factory instanceof Log4jContextFactory) {
            final ContextSelector selector = ((Log4jContextFactory) factory).getSelector();
            return selector;
        }
        return null;
    }

    /**
     * Unregisters all MBeans associated with the specified logger context (including MBeans for {@code LoggerConfig}s
     * and {@code Appender}s from the platform MBean server.
     *
     * @param loggerContextName name of the logger context to unregister
     */
    public static void unregisterLoggerContext(final String loggerContextName) {
        if (isJmxDisabled()) {
            LOGGER.debug("JMX disabled for Log4j2. Not unregistering MBeans.");
            return;
        }
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        unregisterLoggerContext(loggerContextName, mbs);
    }

    /**
     * Unregisters all MBeans associated with the specified logger context (including MBeans for {@code LoggerConfig}s
     * and {@code Appender}s from the platform MBean server.
     *
     * @param contextName name of the logger context to unregister
     * @param mbs the MBean Server to unregister the instrumented objects from
     */
    public static void unregisterLoggerContext(final String contextName, final MBeanServer mbs) {
        final String pattern = LoggerContextAdminMBean.PATTERN;
        final String search = String.format(pattern, escape(contextName), "*");
        unregisterAllMatching(search, mbs); // unregister context mbean

        // now unregister all MBeans associated with this logger context
        unregisterStatusLogger(contextName, mbs);
        unregisterContextSelector(contextName, mbs);
        unregisterLoggerConfigs(contextName, mbs);
        unregisterAppenders(contextName, mbs);
        unregisterAsyncAppenders(contextName, mbs);
        unregisterAsyncLoggerRingBufferAdmins(contextName, mbs);
        unregisterAsyncLoggerConfigRingBufferAdmins(contextName, mbs);
    }

    private static void registerStatusLogger(final String contextName, final MBeanServer mbs, final Executor executor)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {

        final StatusLoggerAdmin mbean = new StatusLoggerAdmin(contextName, executor);
        register(mbs, mbean, mbean.getObjectName());
    }

    private static void registerContextSelector(final String contextName, final ContextSelector selector,
                                                final MBeanServer mbs, final Executor executor) throws InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException {

        final ContextSelectorAdmin mbean = new ContextSelectorAdmin(contextName, selector);
        register(mbs, mbean, mbean.getObjectName());
    }

    private static void unregisterStatusLogger(final String contextName, final MBeanServer mbs) {
        final String pattern = StatusLoggerAdminMBean.PATTERN;
        final String search = String.format(pattern, escape(contextName), "*");
        unregisterAllMatching(search, mbs);
    }

    private static void unregisterContextSelector(final String contextName, final MBeanServer mbs) {
        final String pattern = ContextSelectorAdminMBean.PATTERN;
        final String search = String.format(pattern, escape(contextName), "*");
        unregisterAllMatching(search, mbs);
    }

    private static void unregisterLoggerConfigs(final String contextName, final MBeanServer mbs) {
        final String pattern = LoggerConfigAdminMBean.PATTERN;
        final String search = String.format(pattern, escape(contextName), "*");
        unregisterAllMatching(search, mbs);
    }

    private static void unregisterContexts(final MBeanServer mbs) {
        final String pattern = LoggerContextAdminMBean.PATTERN;
        final String search = String.format(pattern, "*");
        unregisterAllMatching(search, mbs);
    }

    private static void unregisterAppenders(final String contextName, final MBeanServer mbs) {
        final String pattern = AppenderAdminMBean.PATTERN;
        final String search = String.format(pattern, escape(contextName), "*");
        unregisterAllMatching(search, mbs);
    }

    private static void unregisterAsyncAppenders(final String contextName, final MBeanServer mbs) {
        final String pattern = AsyncAppenderAdminMBean.PATTERN;
        final String search = String.format(pattern, escape(contextName), "*");
        unregisterAllMatching(search, mbs);
    }

    private static void unregisterAsyncLoggerRingBufferAdmins(final String contextName, final MBeanServer mbs) {
        final String pattern1 = RingBufferAdminMBean.PATTERN_ASYNC_LOGGER;
        final String search1 = String.format(pattern1, escape(contextName));
        unregisterAllMatching(search1, mbs);
    }

    private static void unregisterAsyncLoggerConfigRingBufferAdmins(final String contextName, final MBeanServer mbs) {
        final String pattern2 = RingBufferAdminMBean.PATTERN_ASYNC_LOGGER_CONFIG;
        final String search2 = String.format(pattern2, escape(contextName), "*");
        unregisterAllMatching(search2, mbs);
    }

    private static void unregisterAllMatching(final String search, final MBeanServer mbs) {
        try {
            final ObjectName pattern = new ObjectName(search);
            final Set<ObjectName> found = mbs.queryNames(pattern, null);
            if (found.isEmpty()) {
                LOGGER.trace("Unregistering but no MBeans found matching '{}'", search);
            } else {
                LOGGER.trace("Unregistering {} MBeans: {}", found.size(), found);
            }
            for (final ObjectName objectName : found) {
                mbs.unregisterMBean(objectName);
            }
        } catch (final Exception ex) {
            LOGGER.error("Could not unregister MBeans for " + search, ex);
        }
    }

    private static void registerLoggerConfigs(final LoggerContext ctx, final MBeanServer mbs, final Executor executor)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {

        final Map<String, LoggerConfig> map = ctx.getConfiguration().getLoggers();
        for (final String name : map.keySet()) {
            final LoggerConfig cfg = map.get(name);
            final LoggerConfigAdmin mbean = new LoggerConfigAdmin(ctx, cfg);
            register(mbs, mbean, mbean.getObjectName());

            if (cfg instanceof AsyncLoggerConfig) {
                final AsyncLoggerConfig async = (AsyncLoggerConfig) cfg;
                final RingBufferAdmin rbmbean = async.createRingBufferAdmin(ctx.getName());
                register(mbs, rbmbean, rbmbean.getObjectName());
            }
        }
    }

    private static void registerAppenders(final LoggerContext ctx, final MBeanServer mbs, final Executor executor)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {

        final Map<String, Appender> map = ctx.getConfiguration().getAppenders();
        for (final String name : map.keySet()) {
            final Appender appender = map.get(name);

            if (appender instanceof AsyncAppender) {
                final AsyncAppender async = ((AsyncAppender) appender);
                final AsyncAppenderAdmin mbean = new AsyncAppenderAdmin(ctx.getName(), async);
                register(mbs, mbean, mbean.getObjectName());
            } else {
                final AppenderAdmin mbean = new AppenderAdmin(ctx.getName(), appender);
                register(mbs, mbean, mbean.getObjectName());
            }
        }
    }

    private static void register(final MBeanServer mbs, final Object mbean, final ObjectName objectName)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        LOGGER.debug("Registering MBean {}", objectName);
        mbs.registerMBean(mbean, objectName);
    }
}
