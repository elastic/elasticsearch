/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.web.UriParts;

import java.util.LinkedHashMap;
import java.util.SequencedCollection;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

import static org.elasticsearch.web.UriParts.DOMAIN;
import static org.elasticsearch.web.UriParts.EXTENSION;
import static org.elasticsearch.web.UriParts.FRAGMENT;
import static org.elasticsearch.web.UriParts.PASSWORD;
import static org.elasticsearch.web.UriParts.PATH;
import static org.elasticsearch.web.UriParts.PORT;
import static org.elasticsearch.web.UriParts.QUERY;
import static org.elasticsearch.web.UriParts.SCHEME;
import static org.elasticsearch.web.UriParts.USERNAME;
import static org.elasticsearch.web.UriParts.USER_INFO;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_INT_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.intValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;

/**
 * A bridge for the function that extracts parts from a URI string.
 * See {@link org.elasticsearch.web.UriParts} for the original logic that is being bridged.
 */
public final class UriPartsFunctionBridge {

    public static LinkedHashMap<String, Class<?>> getAllOutputFields() {
        return UriParts.getUriPartsTypes();
    }

    public static final class UriPartsCollectorImpl extends CompoundOutputEvaluator.OutputFieldsCollector
        implements
            UriParts.UriPartsCollector {
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> domain;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> fragment;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> path;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> extension;
        private final ObjIntConsumer<CompoundOutputEvaluator.RowOutput> port;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> query;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> scheme;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> userInfo;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> username;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> password;

        public UriPartsCollectorImpl(SequencedCollection<String> outputFields) {
            super(outputFields.size());

            BiConsumer<CompoundOutputEvaluator.RowOutput, String> domain = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> fragment = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> path = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> extension = NOOP_STRING_COLLECTOR;
            ObjIntConsumer<CompoundOutputEvaluator.RowOutput> port = NOOP_INT_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> query = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> scheme = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> userInfo = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> username = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> password = NOOP_STRING_COLLECTOR;

            int index = 0;
            for (String outputField : outputFields) {
                switch (outputField) {
                    case DOMAIN:
                        domain = stringValueCollector(index);
                        break;
                    case FRAGMENT:
                        fragment = stringValueCollector(index);
                        break;
                    case PATH:
                        path = stringValueCollector(index);
                        break;
                    case EXTENSION:
                        extension = stringValueCollector(index);
                        break;
                    case PORT:
                        port = intValueCollector(index, value -> value >= 0);
                        break;
                    case QUERY:
                        query = stringValueCollector(index);
                        break;
                    case SCHEME:
                        scheme = stringValueCollector(index);
                        break;
                    case USER_INFO:
                        userInfo = stringValueCollector(index);
                        break;
                    case USERNAME:
                        username = stringValueCollector(index);
                        break;
                    case PASSWORD:
                        password = stringValueCollector(index);
                        break;
                    default:
                        // we may be asked to collect an unknow field, which we only need to ignore and the corresponding block will be
                        // filled with nulls
                }
                index++;
            }

            this.domain = domain;
            this.fragment = fragment;
            this.path = path;
            this.extension = extension;
            this.port = port;
            this.query = query;
            this.scheme = scheme;
            this.userInfo = userInfo;
            this.username = username;
            this.password = password;
        }

        @Override
        public void domain(String domain) {
            this.domain.accept(rowOutput, domain);
        }

        @Override
        public void fragment(String fragment) {
            this.fragment.accept(rowOutput, fragment);
        }

        @Override
        public void path(String path) {
            this.path.accept(rowOutput, path);
        }

        @Override
        public void extension(String extension) {
            this.extension.accept(rowOutput, extension);
        }

        @Override
        public void port(int port) {
            this.port.accept(rowOutput, port);
        }

        @Override
        public void query(String query) {
            this.query.accept(rowOutput, query);
        }

        @Override
        public void scheme(String scheme) {
            this.scheme.accept(rowOutput, scheme);
        }

        @Override
        public void userInfo(String userInfo) {
            this.userInfo.accept(rowOutput, userInfo);
        }

        @Override
        public void username(String username) {
            this.username.accept(rowOutput, username);
        }

        @Override
        public void password(String password) {
            this.password.accept(rowOutput, password);
        }

        @Override
        public void evaluate(String input) {
            UriParts.parse(input, this);
        }
    }
}
