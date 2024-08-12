/*
 * nimbus-jose-jwt
 *
 * Copyright 2012-2016, Connect2id Ltd and contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.nimbusds.jose.util;

import com.nimbusds.jose.shaded.gson.Gson;

/**
 * JSON string helper methods.
 *
 * @author Vladimir Dzhuvinov
 * @version 2022-08-16
 */
public class InnerJSONStringUtils {

    /**
     * Serialises the specified string to a JSON string.
     *
     * @param string The string. Must not be {@code null}.
     *
     * @return The string as JSON string.
     */
    public static String toJSONString(final String string) {
        return new Gson().toJson(string);
    }

    /**
     * Prevents public instantiation.
     */
    private InnerJSONStringUtils() {}
}
