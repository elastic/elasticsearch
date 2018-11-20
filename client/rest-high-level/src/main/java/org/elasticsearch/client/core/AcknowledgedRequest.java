package org.elasticsearch.client.core;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.unit.TimeValue;

/**
 * A rest request that is acknowledged by a master
 */
public class AcknowledgedRequest implements Validatable {

    private TimeValue masterNodeTimeout;
    private TimeValue timeout;

    /**
     * Allows to set the timeout
     * @param timeout timeout as a {@link TimeValue}
     */
    public final void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    public final void setMasterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
    }

    /**
     * The master timeout or null if the server default should be used.
     */
    public TimeValue getMasterNodeTimeout() {
        return masterNodeTimeout;
    }

    /**
     * The timeout or null if the server default should be used.
     */
    public TimeValue getTimeout() {
        return timeout;
    }
}
