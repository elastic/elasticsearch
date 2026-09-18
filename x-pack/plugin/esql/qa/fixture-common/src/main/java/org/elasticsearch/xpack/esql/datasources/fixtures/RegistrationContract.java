/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * What the dataset registration endpoint must refuse, read from {@code registration-contract.properties}.
 *
 * <p>Separate from {@link FixtureDimensions} because the claims are a different shape. A dimension is
 * something to CROSS -- its value interacts with other values, and the contract records which pairs are
 * worth generating together. A refusal does not interact: a setting the validator turns down is turned
 * down whatever else the dataset carries. Declaring one as a dimension would demand a verdict against
 * every other dimension and buy no cell that the single case does not already reach.
 *
 * <p>The load-bearing field is the message. Registration performs no I/O, so the endpoint's whole
 * contract is which settings it can decide about and what it says when it decides against one -- and a
 * test that checks only the status cannot tell a refusal of the setting it varied from a refusal of
 * something it did not. Those two are the same green tick everywhere downstream.
 */
public final class RegistrationContract {

    private static final String RESOURCE = "registration-contract.properties";
    private static final String DEFAULT_FORMAT = "csv";

    /** Every key shape the declaration recognises. A key matching none of these fails the load. */
    private static final Set<String> ATTRIBUTES = Set.of("setting", "value", "message", "emitter", "format");

    /**
     * One registration that must be refused.
     *
     * @param name    the case name, so a failure says which refusal broke
     * @param setting the dataset setting to register
     * @param value   the value to register it with, as the user would write it
     * @param message a substring the refusal must contain
     * @param emitter the symbol the message was read from, so the next reader can check it rather than
     *                trusting that someone did
     * @param format  the format the dataset is registered as
     */
    public record Case(String name, String setting, String value, String message, String emitter, String format) {}

    private final List<Case> cases;

    private RegistrationContract(List<Case> cases) {
        this.cases = List.copyOf(cases);
    }

    private static final RegistrationContract INSTANCE = load();

    public static RegistrationContract get() {
        return INSTANCE;
    }

    /** Every declared refusal, in declaration-name order so a shard slices the same list every run. */
    public List<Case> cases() {
        return cases;
    }

    private static RegistrationContract load() {
        Properties props = new Properties();
        try (InputStream in = RegistrationContract.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(
                    "[" + RESOURCE + "] is not on the classpath; the module reading it must depend on esql:qa:fixture-common"
                );
            }
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read [" + RESOURCE + "]", e);
        }
        return parse(props);
    }

    static RegistrationContract parse(Properties props) {
        Set<String> names = new TreeSet<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("case.") == false) {
                throw new IllegalStateException("unknown key [" + key + "] in [" + RESOURCE + "]; expected 'case.<name>.<attribute>'");
            }
            String rest = key.substring("case.".length());
            int dot = rest.lastIndexOf('.');
            if (dot < 0) {
                throw new IllegalStateException("malformed key [" + key + "]; expected 'case.<name>.<attribute>'");
            }
            String attribute = rest.substring(dot + 1);
            if (ATTRIBUTES.contains(attribute) == false) {
                throw new IllegalStateException(
                    "case [" + rest.substring(0, dot) + "] declares unknown attribute [" + attribute + "]; expected one of " + ATTRIBUTES
                );
            }
            names.add(rest.substring(0, dot));
        }

        List<Case> parsed = new ArrayList<>();
        for (String name : names) {
            String setting = required(props, name, "setting");
            String value = required(props, name, "value");
            String message = required(props, name, "message");
            // The emitter is required rather than helpful. A message with no symbol beside it cannot be
            // re-checked against the code without searching for the string, and a message that has
            // drifted is exactly the one the search will not find.
            String emitter = required(props, name, "emitter");
            String format = props.getProperty("case." + name + ".format", DEFAULT_FORMAT).trim();
            parsed.add(new Case(name, setting, value, message, emitter, format));
        }
        if (parsed.isEmpty()) {
            throw new IllegalStateException("[" + RESOURCE + "] declares no cases; an empty contract asserts nothing and passes");
        }
        return new RegistrationContract(parsed);
    }

    private static String required(Properties props, String name, String attribute) {
        String value = props.getProperty("case." + name + "." + attribute);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("case [" + name + "] declares no [" + attribute + "]");
        }
        return value.trim();
    }
}
