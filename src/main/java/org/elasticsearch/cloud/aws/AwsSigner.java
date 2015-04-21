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
import org.elasticsearch.ElasticsearchIllegalArgumentException;

public class AwsSigner {

    private AwsSigner() {

    }

    /**
     * Add a AWS API Signer.
     * @param signer Signer to use
     * @param configuration AWS Client configuration
     * @throws ElasticsearchIllegalArgumentException if signer does not exist
     */
    public static void configureSigner(String signer, ClientConfiguration configuration)
        throws ElasticsearchIllegalArgumentException {

        if (signer == null) {
            throw new ElasticsearchIllegalArgumentException("[null] signer set");
        }

        try {
            // We check this signer actually exists in AWS SDK
            // It throws a IllegalArgumentException if not found
            SignerFactory.getSignerByTypeAndService(signer, null);
            configuration.setSignerOverride(signer);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchIllegalArgumentException("wrong signer set [" + signer + "]");
        }
    }
}
