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

package org.elasticsearch.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.RestRequest.Method;

import static java.util.Collections.unmodifiableList;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Helper subclass for describing REST handlers that want to register in standard ways.
 */
public abstract class BaseStandardRegistrationsRestHandler extends BaseRestHandler {
    /**
     * Many controllers use these methods.
     */
    protected static final Method[] PUT_AND_POST = new Method[] {PUT, POST};

    private final Method[] methods;
    private final String[] paths;

    /**
     * Constructor for when registering for a single method.
     */
    public BaseStandardRegistrationsRestHandler(RestGlobalContext context, Method method, String... paths) {
        this(context, new Method[] {method}, paths);
    }

    /**
     * Constructor for when registering for multiple methods on the same paths.
     */
    public BaseStandardRegistrationsRestHandler(RestGlobalContext context, Method[] methods, String... paths) {
        super(context);
        this.methods = methods;
        this.paths = paths;
    }

    protected String[] paths() {
        return paths;
    }

    protected Method[] methods() {
        return methods;
    }

    @Override
    public final Collection<Tuple<Method, String>> registrations() {
        List<Tuple<Method, String>> registrations = new ArrayList<>(paths.length * methods.length);
        for (String path : paths) {
            for (Method method : methods) {
                registrations.add(new Tuple<>(method, path));
            }
        }
        return unmodifiableList(registrations);
    }
}
