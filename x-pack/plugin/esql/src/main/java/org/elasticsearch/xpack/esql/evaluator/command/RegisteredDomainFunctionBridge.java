/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.web.RegisteredDomain;

import java.util.LinkedHashMap;
import java.util.SequencedCollection;
import java.util.function.BiConsumer;

import static org.elasticsearch.web.RegisteredDomain.DOMAIN;
import static org.elasticsearch.web.RegisteredDomain.REGISTERED_DOMAIN;
import static org.elasticsearch.web.RegisteredDomain.SUBDOMAIN;
import static org.elasticsearch.web.RegisteredDomain.eTLD;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;

/**
 * A bridge for the function that extracts registered domain parts from an FQDN string.
 * See {@link org.elasticsearch.web.RegisteredDomain} for the original logic that is being bridged.
 */
public final class RegisteredDomainFunctionBridge {

    public static LinkedHashMap<String, Class<?>> getAllOutputFields() {
        return RegisteredDomain.getRegisteredDomainInfoFields();
    }

    public static final class RegisteredDomainCollectorImpl extends CompoundOutputEvaluator.OutputFieldsCollector
        implements
            RegisteredDomain.RegisteredDomainInfoCollector {
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> domain;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> registeredDomain;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> topLevelDomain;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> subdomain;

        public RegisteredDomainCollectorImpl(SequencedCollection<String> outputFields) {
            super(outputFields.size());

            BiConsumer<CompoundOutputEvaluator.RowOutput, String> domain = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> registeredDomain = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> topLevelDomain = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> subdomain = NOOP_STRING_COLLECTOR;

            int index = 0;
            for (String outputField : outputFields) {
                switch (outputField) {
                    case DOMAIN:
                        domain = stringValueCollector(index);
                        break;
                    case REGISTERED_DOMAIN:
                        registeredDomain = stringValueCollector(index);
                        break;
                    case eTLD:
                        topLevelDomain = stringValueCollector(index);
                        break;
                    case SUBDOMAIN:
                        subdomain = stringValueCollector(index);
                        break;
                    default:
                        // unknown field: ignore; corresponding slot remains null
                }
                index++;
            }

            this.domain = domain;
            this.registeredDomain = registeredDomain;
            this.topLevelDomain = topLevelDomain;
            this.subdomain = subdomain;
        }

        @Override
        public void domain(String domain) {
            this.domain.accept(rowOutput, domain);
        }

        @Override
        public void registeredDomain(String registeredDomain) {
            this.registeredDomain.accept(rowOutput, registeredDomain);
        }

        @Override
        public void topLevelDomain(String topLevelDomain) {
            this.topLevelDomain.accept(rowOutput, topLevelDomain);
        }

        @Override
        public void subdomain(String subdomain) {
            this.subdomain.accept(rowOutput, subdomain);
        }

        @Override
        public void evaluate(String input) {
            if (RegisteredDomain.parseRegisteredDomainInfo(input, this) == false) {
                throw new IllegalArgumentException("Invalid domain [" + input + "]");
            }
        }
    }
}
