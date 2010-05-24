/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.elasticsearch.util.http.client;

public class Cookie {
    private String domain;
    private String name;
    private String value;
    private String path;
    private int maxAge;
    private boolean secure;

    public Cookie(String domain, String name, String value, String path, int maxAge, boolean secure) {
        this.domain = domain;
        this.name = name;
        this.value = value;
        this.path = path;
        this.maxAge = maxAge;
        this.secure = secure;
    }

    public String getDomain() {
        return domain;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public String getPath() {
        return path;
    }

    public int getMaxAge() {
        return maxAge;
    }

    public boolean isSecure() {
        return secure;
    }

    @Override
    public String toString() {
        return String.format("Cookie: domain=%s, name=%s, value=%s, path=%s, maxAge=%d, secure=%s",
                domain, name, value, path, maxAge, secure);
    }
}
