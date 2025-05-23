/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParserVisitor;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandLicenseTests extends ESTestCase {
    private static final Logger log = LogManager.getLogger(CommandLicenseTests.class);

    public void testLicenseCheck() {
        var sourceCommand = new LocalRelation(Source.EMPTY, List.of(), null);
        for (var commandName : getCommandClasses().keySet()) {
            Class<? extends LogicalPlan> commandClass = getCommandClasses().get(commandName);
            try {
                var arg = (commandClass == InlineStats.class) ? new Aggregate(Source.EMPTY, sourceCommand, null, null) : sourceCommand;
                checkLicense(commandName, createInstance(commandClass, arg));
            } catch (Exception e) {
                Throwable c = e.getCause();
                fail("Failed to create instance of command class: " + commandClass.getName() + " - " + e.getMessage() + " - " + c);
            }
        }
    }

    public static class TestCheckLicense {
        XPackLicenseState basicLicense = makeLicenseState(License.OperationMode.BASIC);
        XPackLicenseState platinumLicense = makeLicenseState(License.OperationMode.PLATINUM);
        XPackLicenseState enterpriseLicense = makeLicenseState(License.OperationMode.ENTERPRISE);

        private XPackLicenseState licenseLevel(LicenseAware licenseAware) {
            for (XPackLicenseState license : List.of(basicLicense, platinumLicense, enterpriseLicense)) {
                if (licenseAware.licenseCheck(license)) {
                    return license;
                }
            }
            throw new IllegalArgumentException("No license level is supported by " + licenseAware.getClass().getName());
        }
    }

    private static XPackLicenseState makeLicenseState(License.OperationMode mode) {
        return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(mode, true, null));
    }

    private static void checkLicense(String commandName, LogicalPlan command) throws Exception {
        log.info("Running function license checks");
        TestCheckLicense checkLicense = new TestCheckLicense();
        if (command instanceof LicenseAware licenseAware) {
            log.info("Command " + commandName + " implements LicenseAware.");
            saveLicenseState(commandName, command, checkLicense.licenseLevel(licenseAware));
        } else {
            log.info("Command " + commandName + " does not implement LicenseAware.");
            saveLicenseState(commandName, command, checkLicense.basicLicense);
        }
    }

    private static void saveLicenseState(String name, LogicalPlan command, XPackLicenseState licenseState) throws Exception {
        DocsV3Support.CommandsDocsSupport docs = new DocsV3Support.CommandsDocsSupport(
            name.toLowerCase(Locale.ROOT),
            CommandLicenseTests.class,
            command,
            licenseState
        );
        docs.renderDocs();
    }

    // Find all command classes, by looking at the public methods of the EsqlBaseParserVisitor
    private static Map<String, Class<? extends LogicalPlan>> getCommandClasses() {
        Map<String, Class<? extends LogicalPlan>> commandClasses = new TreeMap<>();
        Pattern pattern = Pattern.compile("visit(\\w+)Command");
        String planPackage = "org.elasticsearch.xpack.esql.plan.logical";
        Map<String, String> commandClassNameMapper = Map.of(
            "Where",
            "Filter",
            "Inlinestats",
            "InlineStats",
            "Rrf",
            "RrfScoreEval",
            "Sort",
            "OrderBy",
            "Stats",
            "Aggregate",
            "Join",
            "LookupJoin"
        );
        Map<String, String> commandNameMapper = Map.of("ChangePoint", "CHANGE_POINT", "LookupJoin", "LOOKUP_JOIN", "MvExpand", "MV_EXPAND");
        Map<String, String> commandPackageMapper = Map.of("Rerank", planPackage + ".inference", "LookupJoin", planPackage + ".join");
        Set<String> ignoredClasses = Set.of("Processing", "TimeSeries", "Completion", "Source", "From", "Row");

        for (Method method : EsqlBaseParserVisitor.class.getMethods()) {
            String methodName = method.getName();
            Matcher matcher = pattern.matcher(methodName);
            if (matcher.matches()) {
                String className = matcher.group(1);
                if (ignoredClasses.contains(className)) {
                    continue;
                }
                String commandName = commandNameMapper.getOrDefault(className, className.toUpperCase(Locale.ROOT));
                if (commandClassNameMapper.containsKey(className)) {
                    className = commandClassNameMapper.get(className);
                    if (commandNameMapper.containsKey(className)) {
                        commandName = commandNameMapper.get(className);
                    }
                }
                try {
                    String fullClassName = commandPackageMapper.getOrDefault(className, planPackage) + "." + className;
                    Class<?> candidateClass = Class.forName(fullClassName);

                    if (LogicalPlan.class.isAssignableFrom(candidateClass)) {
                        commandClasses.put(commandName, candidateClass.asSubclass(LogicalPlan.class));
                    } else {
                        log.info("Class " + className + " does NOT extend LogicalPlan.");
                    }
                } catch (ClassNotFoundException e) {
                    log.info("Class " + className + " not found.");
                }
            }
        }
        return commandClasses;
    }

    private static LogicalPlan createInstance(Class<? extends LogicalPlan> clazz, LogicalPlan child) throws InvocationTargetException,
        InstantiationException, IllegalAccessException {
        Source source = Source.EMPTY;

        // hard coded cases where the first two parameters are not Source and child LogicalPlan
        switch (clazz.getSimpleName()) {
            case "Grok" -> {
                return new Grok(source, child, null, null, List.of());
            }
            case "Fork" -> {
                return new Fork(source, List.of(child, child), List.of());
            }
            case "Sample" -> {
                return new Sample(source, null, null, child);
            }
            case "LookupJoin" -> {
                return new LookupJoin(source, child, child, List.of());
            }
            case "Limit" -> {
                return new Limit(source, null, child);
            }
        }

        // For all others, find the constructor that takes Source and LogicalPlan as the first two parameters
        Constructor<?>[] constructors = clazz.getConstructors();

        Constructor<?> constructor = Arrays.stream(constructors).filter(c -> {
            Class<?>[] params = c.getParameterTypes();
            return params.length > 1 && Source.class.isAssignableFrom(params[0]) && LogicalPlan.class.isAssignableFrom(params[1]);
        })
            .min(Comparator.comparingInt(c -> c.getParameterTypes().length))
            .orElseThrow(() -> new IllegalArgumentException("No suitable constructor found for class " + clazz.getName()));

        Class<?>[] paramTypes = constructor.getParameterTypes();
        Object[] args = new Object[paramTypes.length];
        args[0] = source;
        args[1] = child;
        log.info("Creating instance of " + clazz.getName() + " with constructor: " + constructor);
        return (LogicalPlan) constructor.newInstance(args);
    }
}
