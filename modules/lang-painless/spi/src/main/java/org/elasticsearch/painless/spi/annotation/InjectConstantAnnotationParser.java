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

package org.elasticsearch.painless.spi.annotation;

import java.util.ArrayList;
import java.util.Map;

public class InjectConstantAnnotationParser implements WhitelistAnnotationParser {

    public static final InjectConstantAnnotationParser INSTANCE = new InjectConstantAnnotationParser();

    private InjectConstantAnnotationParser() {}

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException("[@inject_constant] requires at least one name to inject");
        }
        ArrayList<String> argList = new ArrayList<>(arguments.size());
        for (int i = 1; i <= arguments.size(); i++) {
            String argNum = Integer.toString(i);
            if (arguments.containsKey(argNum) == false) {
                throw new IllegalArgumentException("[@inject_constant] missing argument number [" + argNum + "]");
            }
            argList.add(arguments.get(argNum));
        }

        return new InjectConstantAnnotation(argList);
    }
}
