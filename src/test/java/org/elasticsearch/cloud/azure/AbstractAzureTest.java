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

package org.elasticsearch.cloud.azure;

import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 */
public abstract class AbstractAzureTest extends ElasticsearchIntegrationTest {

    /**
     * Annotation for tests that require Azure to run. Azure tests are disabled by default.
     * <p/>
     * To enable test add -Dtests.azure=true -Des.config=/path/to/elasticsearch.yml
     * <p/>
     * The elasticsearch.yml file should contain the following keys
     * <pre>
      cloud:
          azure:
              keystore: FULLPATH-TO-YOUR-KEYSTORE
              password: YOUR-PASSWORD
              subscription_id: YOUR-AZURE-SUBSCRIPTION-ID
              service_name: YOUR-AZURE-SERVICE-NAME

      discovery:
              type: azure

      repositories:
          azure:
              account: "yourstorageaccount"
              key: "storage key"
              container: "container name"
     * </pre>
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = SYSPROP_AZURE)
    public @interface AzureTest {
    }

    /**
     */
    public static final String SYSPROP_AZURE = "tests.azure";

}
