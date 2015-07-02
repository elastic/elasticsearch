/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.monitor.probe;

import java.util.Collection;
import java.util.Iterator;

/**
 * A composite probe acts as a probe of type P but asks one or more underlying probes for data,
 * masking unsupported exception if necessary.
 */
public abstract class CompositeProbe<P extends Probe> {

    private final Collection<P> probes;
    private final ProbeStrategy strategy;

    public CompositeProbe(Collection<P> probes, Strategy strategy) {
        this.probes = probes;
        switch (strategy) {
            case FIRST_WIN:
                this.strategy = new FirstWinStrategy();
                break;
            default:
                this.strategy = new LastWinStrategy();
        }
    }

    public CompositeProbe(Collection<P> probes) {
        this(probes, Strategy.LAST_WIN);
    }

    protected <T> T executeOnProbes(Callback<T> callback) {
        T result = null;

        Iterator<P> it = probes.iterator();
        while (it.hasNext()) {
            P probe = it.next();
            try {
                result = callback.executeProbeOperation(probe);
                if (!strategy.shouldContinue(result, !it.hasNext())) {
                    break;
                }
            } catch (Exception e) {
                if (!strategy.shouldContinue(e, !it.hasNext())) {
                    break;
                }
            }
        }
        return result;
    }

    protected abstract class Callback<T> {
        protected abstract T executeProbeOperation(final P probe) throws UnsupportedOperationException;
    }

    public enum Strategy {
        LAST_WIN,
        FIRST_WIN,
    }

    interface ProbeStrategy {

        boolean shouldContinue(Object lastResult, boolean isLastProbe);

        boolean shouldContinue(Exception lastException, boolean isLastProbe);
    }

    /**
     * Asks all probes for data, overwriting any previous value
     * every time a probe returns a non-null data.
     */
    class LastWinStrategy implements ProbeStrategy {

        @Override
        public boolean shouldContinue(Object lastResult, boolean isLastProbe) {
            return !isLastProbe;
        }

        @Override
        public boolean shouldContinue(Exception lastException, boolean isLastProbe) {
            return !isLastProbe && (lastException instanceof UnsupportedOperationException);
        }
    }

    /**
     * Stops at the first probe that returns a non-null data.
     */
    class FirstWinStrategy implements ProbeStrategy {

        @Override
        public boolean shouldContinue(Object lastResult, boolean isLastProbe) {
            // continue on null results, otherwise stop
            return (lastResult == null);
        }

        @Override
        public boolean shouldContinue(Exception lastException, boolean isLastProbe) {
            // continue if the probe does not support this data
            return (lastException instanceof UnsupportedOperationException);
        }
    }
}
