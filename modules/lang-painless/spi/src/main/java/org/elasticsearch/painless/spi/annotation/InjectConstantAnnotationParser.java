/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
