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

package org.elasticsearch.cloud.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.SignerFactory;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class AwsSigner {

    private static final ESLogger logger = Loggers.getLogger(AwsSigner.class);

    private AwsSigner() {

    }

    protected static void validateSignerType(String signer) throws IllegalArgumentException {
        if (signer == null) {
            throw new IllegalArgumentException("[null] signer set");
        }

        try {
            // We check this signer actually exists in AWS SDK
            // It throws a IllegalArgumentException if not found
            SignerFactory.getSignerByTypeAndService(signer, null);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("wrong signer set [" + signer + "]");
        }
    }

    /**
     * Add a AWS API Signer.
     * @param signer Signer to use
     * @param configuration AWS Client configuration
     */
    public static void configureSigner(String signer, ClientConfiguration configuration) {
        try {
            validateSignerType(signer);
        } catch (IllegalArgumentException e) {
            logger.warn(e.getMessage());
        }

        configuration.setSignerOverride(signer);
    }
}
