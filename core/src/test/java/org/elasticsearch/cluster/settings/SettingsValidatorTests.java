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

package org.elasticsearch.cluster.settings;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public class SettingsValidatorTests extends ESTestCase {

    @Test
    public void testValidators() throws Exception {
        assertThat(Validator.EMPTY.validate("", "anything goes", null), nullValue());

        assertThat(Validator.TIME.validate("", "10m", null), nullValue());
        assertThat(Validator.TIME.validate("", "10g", null), notNullValue());
        assertThat(Validator.TIME.validate("", "bad timing", null), notNullValue());

        assertThat(Validator.BYTES_SIZE.validate("", "10m", null), nullValue());
        assertThat(Validator.BYTES_SIZE.validate("", "10g", null), nullValue());
        assertThat(Validator.BYTES_SIZE.validate("", "bad", null), notNullValue());

        assertThat(Validator.FLOAT.validate("", "10.2", null), nullValue());
        assertThat(Validator.FLOAT.validate("", "10.2.3", null), notNullValue());

        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "10.2", null), nullValue());
        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "0.0", null), nullValue());
        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "-1.0", null), notNullValue());
        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "10.2.3", null), notNullValue());

        assertThat(Validator.DOUBLE.validate("", "10.2", null), nullValue());
        assertThat(Validator.DOUBLE.validate("", "10.2.3", null), notNullValue());

        assertThat(Validator.DOUBLE_GTE_2.validate("", "10.2", null), nullValue());
        assertThat(Validator.DOUBLE_GTE_2.validate("", "2.0", null), nullValue());
        assertThat(Validator.DOUBLE_GTE_2.validate("", "1.0", null), notNullValue());
        assertThat(Validator.DOUBLE_GTE_2.validate("", "10.2.3", null), notNullValue());

        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "10.2", null), nullValue());
        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "0.0", null), nullValue());
        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "-1.0", null), notNullValue());
        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "10.2.3", null), notNullValue());

        assertThat(Validator.INTEGER.validate("", "10", null), nullValue());
        assertThat(Validator.INTEGER.validate("", "10.2", null), notNullValue());

        assertThat(Validator.INTEGER_GTE_2.validate("", "2", null), nullValue());
        assertThat(Validator.INTEGER_GTE_2.validate("", "1", null), notNullValue());
        assertThat(Validator.INTEGER_GTE_2.validate("", "0", null), notNullValue());
        assertThat(Validator.INTEGER_GTE_2.validate("", "10.2.3", null), notNullValue());

        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "2", null), nullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "1", null), nullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "0", null), nullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "-1", null), notNullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "10.2", null), notNullValue());

        assertThat(Validator.POSITIVE_INTEGER.validate("", "2", null), nullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "1", null), nullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "0", null), notNullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "-1", null), notNullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "10.2", null), notNullValue());

        assertThat(Validator.PERCENTAGE.validate("", "asdasd", null), notNullValue());
        assertThat(Validator.PERCENTAGE.validate("", "-1", null), notNullValue());
        assertThat(Validator.PERCENTAGE.validate("", "20", null), notNullValue());
        assertThat(Validator.PERCENTAGE.validate("", "-1%", null), notNullValue());
        assertThat(Validator.PERCENTAGE.validate("", "101%", null), notNullValue());
        assertThat(Validator.PERCENTAGE.validate("", "100%", null), nullValue());
        assertThat(Validator.PERCENTAGE.validate("", "99%", null), nullValue());
        assertThat(Validator.PERCENTAGE.validate("", "0%", null), nullValue());

        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "asdasd", null), notNullValue());
        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "20", null), notNullValue());
        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "20mb", null), nullValue());
        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "-1%", null), notNullValue());
        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "101%", null), notNullValue());
        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "100%", null), nullValue());
        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "99%", null), nullValue());
        assertThat(Validator.BYTES_SIZE_OR_PERCENTAGE.validate("", "0%", null), nullValue());
    }

    @Test
    public void testDynamicValidators() throws Exception {
        DynamicSettings.Builder ds = new DynamicSettings.Builder();
        ds.addSetting("my.test.*", Validator.POSITIVE_INTEGER);
        String valid = ds.build().validateDynamicSetting("my.test.setting", "-1", null);
        assertThat(valid, equalTo("the value of the setting my.test.setting must be a positive integer"));
    }
}
