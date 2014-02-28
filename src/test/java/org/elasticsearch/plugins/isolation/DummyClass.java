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
package org.elasticsearch.plugins.isolation;

import java.util.Properties;

public class DummyClass {

    static final String name;

    static {
        Properties sysProps = System.getProperties();
        // make sure to get a string even when dealing with null
        name = "" + sysProps.getProperty("es.test.isolated.plugin.name");
        sysProps.setProperty("es.test.isolated.plugin.instantiated", "" + DummyClass.class.hashCode());
        Integer count = Integer.getInteger("es.test.isolated.plugin.count");
        if (count == null) {
            count = Integer.valueOf(0);
        }

        count = count + 1;

        sysProps.setProperty("es.test.isolated.plugin.count", count.toString());

        String prop = sysProps.getProperty("es.test.isolated.plugin.instantiated.hashes");
        if (prop == null) {
            prop = "";
        }

        prop = prop + DummyClass.class.hashCode() + " ";
        sysProps.setProperty("es.test.isolated.plugin.instantiated.hashes", prop);
        sysProps.setProperty("es.test.isolated.plugin.read.name",  name);
    }

}
