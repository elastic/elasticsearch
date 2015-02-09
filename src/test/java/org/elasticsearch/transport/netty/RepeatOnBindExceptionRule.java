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
package org.elasticsearch.transport.netty;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.transport.BindTransportException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A helper rule to catch all BindTransportExceptions
 * and rerun the test for a configured number of times
 */
public class RepeatOnBindExceptionRule implements TestRule {

    private ESLogger logger;
    private int retryCount;

    /**
     *
     * @param logger the es logger from the test class
     * @param retryCount number of amounts to try a single test before failing
     */
    public RepeatOnBindExceptionRule(ESLogger logger, int retryCount) {
        this.logger = logger;
        this.retryCount = retryCount;
    }

    @Override
    public Statement apply(final Statement base, Description description) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Throwable caughtThrowable = null;

                for (int i = 0; i < retryCount; i++) {
                    try {
                        base.evaluate();
                        return;
                    } catch (BindTransportException t) {
                        caughtThrowable = t;
                        logger.info("Bind exception occurred, rerunning the test after [{}] failures", t, i+1);
                    }
                }
                logger.error("Giving up after [{}] failures... marking test as failed", retryCount);
                throw caughtThrowable;
            }
        };

    }
}
