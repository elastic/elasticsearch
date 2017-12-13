/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.capabilities.Unresolvable;
import org.elasticsearch.xpack.sql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.List;

public class UnresolvedFunction extends Function implements Unresolvable {

    private final String name;
    private final boolean distinct;
    private final String unresolvedMsg;
    // flag to indicate analysis has been applied and there's no point in doing it again
    // this is an optimization to prevent searching for a better unresolved message over and over again
    private final boolean analyzed;

    public UnresolvedFunction(Location location, String name, boolean distinct, List<Expression> children) {
        this(location, name, distinct, children, false, null);
    }

    /**
     * Constructor used for specifying a more descriptive message (typically 'did you mean') instead of the default one.
     */
    public UnresolvedFunction(Location location, String name, boolean distinct, List<Expression> children, boolean analyzed, String unresolvedMessage) {
        super(location, children);
        this.name = name;
        this.distinct = distinct;
        this.analyzed = analyzed;
        this.unresolvedMsg = unresolvedMessage == null ? errorMessage(name, null) : unresolvedMessage;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String functionName() {
        return name;
    }

    public boolean distinct() {
        return distinct;
    }

    public boolean analyzed() {
        return analyzed;
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("dataType", this);
    }

    @Override
    public boolean nullable() {
        throw new UnresolvedException("nullable", this);
    }

    @Override
    public Attribute toAttribute() {
        throw new UnresolvedException("attribute", this);
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + functionName() + functionArgs();
    }

    public static String errorMessage(String name, List<String> potentialMatches) {
        String msg = "Unknown function [" + name + "]";
        if (!CollectionUtils.isEmpty(potentialMatches)) {
            msg += ", did you mean "
                    + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]" : "any of " + potentialMatches.toString()) + "?";
        }
        return msg;
    }
}