package org.elasticsearch.bootstrap.plugins;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.elasticsearch.plugins.PluginLogger;

public final class Log4jPluginLogger extends ExtendedLoggerWrapper implements PluginLogger {
    private final ExtendedLoggerWrapper logger;

    private static final String FQCN = Log4jPluginLogger.class.getName();

    private Log4jPluginLogger(final Logger logger) {
        super((AbstractLogger) logger, logger.getName(), logger.getMessageFactory());
        this.logger = this;
    }

    /**
     * Returns a custom Logger using the fully qualified name of the Class as
     * the Logger name.
     *
     * @param loggerName The Class name that should be used as the Logger name.
     *                   If null it will default to the calling class.
     * @return The custom Logger.
     */
    public static Log4jPluginLogger getLogger(final String loggerName) {
        final Logger wrapped = LogManager.getLogger(loggerName);
        return new Log4jPluginLogger(wrapped);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     *
     * @param message the message object to log.
     */
    @Override
    public void error(final String message) {
        logger.logIfEnabled(FQCN, Level.ERROR, null, message, (Throwable) null);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     *
     * @param message the message object to log.
     */
    @Override
    public void warn(final String message) {
        logger.logIfEnabled(FQCN, Level.WARN, null, message, (Throwable) null);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     *
     * @param message the message object to log.
     */
    @Override
    public void info(final String message) {
        logger.logIfEnabled(FQCN, Level.INFO, null, message, (Throwable) null);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     *
     * @param message the message object to log.
     */
    @Override
    public void debug(final String message) {
        logger.logIfEnabled(FQCN, Level.DEBUG, null, message, (Throwable) null);
    }

}
