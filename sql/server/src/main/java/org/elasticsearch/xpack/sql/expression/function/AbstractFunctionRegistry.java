/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.Node;
import org.elasticsearch.xpack.sql.tree.NodeUtils;
import org.elasticsearch.xpack.sql.tree.NodeUtils.NodeInfo;
import org.elasticsearch.xpack.sql.util.Assert;
import org.elasticsearch.xpack.sql.util.StringUtils;

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
    public Function resolveFunction(UnresolvedFunction ur) {
        FunctionDefinition def = defs.get(normalize(ur.name()));
        if (def == null) {
            throw new SqlIllegalArgumentException("Cannot find function %s; this should have been caught during analysis", ur.name());
        }
        return createInstance(def.clazz(), ur);
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
        Pattern p = Strings.hasText(pattern) ? Pattern.compile(normalize(pattern)) : null;
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
    private static Function createInstance(Class<? extends Function> clazz, UnresolvedFunction ur) {
        NodeInfo info = NodeUtils.info((Class<? extends Node>) clazz);
        Class<?> exp = ur.children().size() == 1 ? Expression.class : List.class;
        Object expVal = exp == Expression.class ? ur.children().get(0) : ur.children();

        boolean distinctAware = true;
        boolean noArgument = false;
        // distinct ctor
        if (!Arrays.equals(new Class[] { Location.class, exp, boolean.class }, info.ctr.getParameterTypes())) {
            if (ur.distinct()) {
                throw new ParsingException(ur.location(), "Function [%s] does not support DISTINCT yet it was specified", ur.name());
            }
            distinctAware = false;
            
            // might be a constant function
            if (expVal instanceof List && ((List) expVal).isEmpty()) {
                noArgument = Arrays.equals(new Class[] { Location.class }, info.ctr.getParameterTypes());
            }
            // distinctless
            else if (!Arrays.equals(new Class[] { Location.class, exp }, info.ctr.getParameterTypes())) {
                throw new SqlIllegalArgumentException("No constructor with signature [%s, %s (,%s)?] found for [%s]",
                        Location.class, exp, boolean.class, clazz.getTypeName());
            }
        }
        
        try {
            Object[] args = noArgument ? new Object[] { ur.location() } : (distinctAware ? new Object[] { ur.location(), expVal, ur.distinct() } : new Object[] { ur.location(), expVal }); 
            return (Function) info.ctr.newInstance(args);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new SqlIllegalArgumentException(ex, "Cannot create instance of function %s", ur.name());
        }
    }
}