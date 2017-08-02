/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.aware.DistinctAware;
import org.elasticsearch.xpack.sql.expression.function.aware.TimeZoneAware;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.session.SqlSettings;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.Node;
import org.elasticsearch.xpack.sql.tree.NodeUtils;
import org.elasticsearch.xpack.sql.tree.NodeUtils.NodeInfo;
import org.elasticsearch.xpack.sql.util.Assert;
import org.elasticsearch.xpack.sql.util.StringUtils;
import org.joda.time.DateTimeZone;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

abstract class AbstractFunctionRegistry implements FunctionRegistry {

    protected final Map<String, FunctionDefinition> defs = new LinkedHashMap<>();

    {
        for (Class<? extends Function> f : functions()) {
            FunctionDefinition def = def(f, aliases());
            defs.put(def.name(), def);
            for (String alias : def.aliases()) {
                Assert.isTrue(defs.containsKey(alias) == false, "Alias %s already exists", alias);
                defs.put(alias, def);
            }
        }
    }

    //TODO: change this to some type of auto discovery or auto creation of the discovery (annotation or the like)
    protected abstract Collection<Class<? extends Function>> functions();
    
    protected abstract Map<String, String> aliases();

    
    @Override
    public Function resolveFunction(UnresolvedFunction ur, SqlSettings settings) {
        FunctionDefinition def = defs.get(normalize(ur.name()));
        if (def == null) {
            throw new SqlIllegalArgumentException("Cannot find function %s; this should have been caught during analysis", ur.name());
        }
        return createInstance(def.clazz(), ur, settings);
    }

    @Override
    public boolean functionExists(String name) {
        return defs.containsKey(normalize(name));
    }

    @Override
    public Collection<FunctionDefinition> listFunctions() {
        return defs.entrySet().stream()
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(), e.getValue().clazz()))
                .collect(toList());
    }

    @Override
    public Collection<FunctionDefinition> listFunctions(String pattern) {
        Pattern p = Strings.hasText(pattern) ? StringUtils.likeRegex(normalize(pattern)) : null;
        return defs.entrySet().stream()
                .filter(e -> p == null || p.matcher(e.getKey()).matches())
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(), e.getValue().clazz()))
                .collect(toList());
    }

    private static FunctionDefinition def(Class<? extends Function> function, Map<String, String> aliases) {
        String primaryName = normalize(function.getSimpleName());
        List<String> al = aliases.entrySet().stream()
                .filter(e -> primaryName.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(toList());

        return new FunctionDefinition(primaryName, al, function);
    }

    protected static String normalize(String name) {
        // translate CamelCase to camel_case
        return StringUtils.camelCaseToUnderscore(name);
    }

    @SuppressWarnings("rawtypes")
    private static Function createInstance(Class<? extends Function> clazz, UnresolvedFunction ur, SqlSettings settings) {
        NodeInfo info = NodeUtils.info((Class<? extends Node>) clazz);
        Class<?> exp = ur.children().size() == 1 ? Expression.class : List.class;
        Object expVal = exp == Expression.class ? ur.children().get(0) : ur.children();

        boolean noExpression = false;
        boolean distinctAware = DistinctAware.class.isAssignableFrom(clazz);
        boolean timezoneAware = TimeZoneAware.class.isAssignableFrom(clazz);

        // check constructor signature
        
        
        // validate distinct ctor
        if (!distinctAware && ur.distinct()) {
            throw new ParsingException(ur.location(), "Function [%s] does not support DISTINCT yet it was specified", ur.name());
        }
        
        List<Class> ctorSignature = new ArrayList<>();
        ctorSignature.add(Location.class);
        
        // might be a constant function
        if (expVal instanceof List && ((List) expVal).isEmpty()) {
            noExpression = Arrays.equals(new Class[] { Location.class }, info.ctr.getParameterTypes());
        }
        else {
            ctorSignature.add(exp);
        }

        // aware stuff
        if (distinctAware) {
            ctorSignature.add(boolean.class);
        }
        if (timezoneAware) {
            ctorSignature.add(DateTimeZone.class);
        }
        
        // validate
        Assert.isTrue(Arrays.equals(ctorSignature.toArray(new Class[ctorSignature.size()]), info.ctr.getParameterTypes()),
                "No constructor with signature %s found for [%s]", ctorSignature, clazz.getTypeName());
        
        // now add the actual values
        try {
            List<Object> args = new ArrayList<>();

            // always add location first
            args.add(ur.location());

            // has multiple arguments
            if (!noExpression) {
                args.add(expVal);
                if (distinctAware) {
                    args.add(ur.distinct());
                }
                if (timezoneAware) {
                    args.add(settings.timeZone());
                }
            } 
            return (Function) info.ctr.newInstance(args.toArray());
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new SqlIllegalArgumentException(ex, "Cannot create instance of function %s", ur.name());
        }
    }
}