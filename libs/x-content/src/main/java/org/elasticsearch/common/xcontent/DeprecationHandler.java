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

package org.elasticsearch.common.xcontent;

/**
 * Callback for notifying the creator of the {@link XContentParser} that
 * parsing hit a deprecated field.
 */
public interface DeprecationHandler {
    /**
     * Throws an {@link UnsupportedOperationException} when parsing hits a
     * deprecated field. Use this when creating an {@link XContentParser}
     * that won't interact with deprecation logic at all or when you want
     * to fail fast when parsing deprecated fields.
     */
    DeprecationHandler THROW_UNSUPPORTED_OPERATION = new DeprecationHandler() {
        @Override
        public void usedDeprecatedField(String usedName, String replacedWith) {
            throw new UnsupportedOperationException("deprecated fields not supported here but got ["
                + usedName + "] which is a deprecated name for [" + replacedWith + "]");
        }
        @Override
        public void usedDeprecatedName(String usedName, String modernName) {
            throw new UnsupportedOperationException("deprecated fields not supported here but got ["
                + usedName + "] which has been replaced with [" + modernName + "]");
        }
    };

    /**
     * Called when the provided field name matches a deprecated name for the field.
     * @param usedName the provided field name
     * @param modernName the modern name for the field
     */
    void usedDeprecatedName(String usedName, String modernName);

    /**
     * Called when the provided field name matches the current field but the entire
     * field has been marked as deprecated.
     * @param usedName the provided field name
     * @param replacedWith the name of the field that replaced this field
     */
    void usedDeprecatedField(String usedName, String replacedWith);
}
