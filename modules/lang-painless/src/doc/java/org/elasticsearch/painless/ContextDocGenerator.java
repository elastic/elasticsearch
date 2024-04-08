/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.painless.action.PainlessContextClassBindingInfo;
import org.elasticsearch.painless.action.PainlessContextClassInfo;
import org.elasticsearch.painless.action.PainlessContextConstructorInfo;
import org.elasticsearch.painless.action.PainlessContextFieldInfo;
import org.elasticsearch.painless.action.PainlessContextInfo;
import org.elasticsearch.painless.action.PainlessContextInstanceBindingInfo;
import org.elasticsearch.painless.action.PainlessContextMethodInfo;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The gradle task generateContextDoc uses ContextDocGenerator to rebuild
 * the Painless API documentation from a clean state after the
 * existing documentation is deleted. The following pages are generated:
 * <ul>
 *     <li>An index page with each context and links to the APIs</li>
 *     <li>A high-level overview page of shared API for all contexts</li>
 *     <li>A detailed page per package per context of shared API for all contexts</li>
 *     <li>A high-level overview page of specialized API for each context</li>
 *     <li>A detailed page per package per context of specialized API for each context</li>
 * </ul>
 * Use the docs build to generate HTML pages from the resultant asciidoc files.
 */
public final class ContextDocGenerator {

    private static final String SHARED_HEADER = "painless-api-reference-shared";
    private static final String SHARED_NAME = "Shared";

    public static void main(String[] args) throws IOException {
        List<PainlessContextInfo> contextInfos = ContextGeneratorCommon.getContextInfos();
        Set<Object> sharedStaticInfos = createSharedStatics(contextInfos);
        Set<PainlessContextClassInfo> sharedClassInfos = createSharedClasses(contextInfos);

        Path rootDir = resetRootDir();

        Path sharedDir = createSharedDir(rootDir);
        List<Object> staticInfos = sortStaticInfos(Collections.emptySet(), new ArrayList<>(sharedStaticInfos));
        List<PainlessContextClassInfo> classInfos = sortClassInfos(Collections.emptySet(), new ArrayList<>(sharedClassInfos));
        Map<String, String> javaNamesToDisplayNames = getDisplayNames(classInfos);
        printSharedIndexPage(sharedDir, javaNamesToDisplayNames, staticInfos, classInfos);
        printSharedPackagesPages(sharedDir, javaNamesToDisplayNames, classInfos);

        Set<PainlessContextInfo> isSpecialized = new HashSet<>();

        for (PainlessContextInfo contextInfo : contextInfos) {
            staticInfos = createContextStatics(contextInfo);
            staticInfos = sortStaticInfos(sharedStaticInfos, staticInfos);
            classInfos = sortClassInfos(sharedClassInfos, new ArrayList<>(contextInfo.getClasses()));

            if (staticInfos.isEmpty() == false || classInfos.isEmpty() == false) {
                Path contextDir = createContextDir(rootDir, contextInfo);
                isSpecialized.add(contextInfo);
                javaNamesToDisplayNames = getDisplayNames(contextInfo.getClasses());
                printContextIndexPage(contextDir, javaNamesToDisplayNames, contextInfo, staticInfos, classInfos);
                printContextPackagesPages(contextDir, javaNamesToDisplayNames, sharedClassInfos, contextInfo, classInfos);
            }
        }

        printRootIndexPage(rootDir, contextInfos, isSpecialized);
    }

    private static Set<Object> createSharedStatics(List<PainlessContextInfo> contextInfos) {
        Map<Object, Integer> staticInfoCounts = new HashMap<>();

        for (PainlessContextInfo contextInfo : contextInfos) {
            for (PainlessContextMethodInfo methodInfo : contextInfo.getImportedMethods()) {
                staticInfoCounts.merge(methodInfo, 1, Integer::sum);
            }

            for (PainlessContextClassBindingInfo classBindingInfo : contextInfo.getClassBindings()) {
                staticInfoCounts.merge(classBindingInfo, 1, Integer::sum);
            }

            for (PainlessContextInstanceBindingInfo instanceBindingInfo : contextInfo.getInstanceBindings()) {
                staticInfoCounts.merge(instanceBindingInfo, 1, Integer::sum);
            }
        }

        return staticInfoCounts.entrySet()
            .stream()
            .filter(e -> e.getValue() == contextInfos.size())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    private static List<Object> createContextStatics(PainlessContextInfo contextInfo) {
        List<Object> staticInfos = new ArrayList<>();

        staticInfos.addAll(contextInfo.getImportedMethods());
        staticInfos.addAll(contextInfo.getClassBindings());
        staticInfos.addAll(contextInfo.getInstanceBindings());

        return staticInfos;
    }

    private static Set<PainlessContextClassInfo> createSharedClasses(List<PainlessContextInfo> contextInfos) {
        Map<PainlessContextClassInfo, Integer> classInfoCounts = new HashMap<>();

        for (PainlessContextInfo contextInfo : contextInfos) {
            for (PainlessContextClassInfo classInfo : contextInfo.getClasses()) {
                classInfoCounts.merge(classInfo, 1, Integer::sum);
            }
        }

        return classInfoCounts.entrySet()
            .stream()
            .filter(e -> e.getValue() == contextInfos.size())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @SuppressForbidden(reason = "resolve api docs directory with environment")
    private static Path resetRootDir() throws IOException {
        Path rootDir = PathUtils.get("../../docs/painless/painless-api-reference");
        IOUtils.rm(rootDir);
        Files.createDirectories(rootDir);

        return rootDir;
    }

    private static Path createSharedDir(Path rootDir) throws IOException {
        Path sharedDir = rootDir.resolve(SHARED_HEADER);
        Files.createDirectories(sharedDir);

        return sharedDir;
    }

    private static Path createContextDir(Path rootDir, PainlessContextInfo info) throws IOException {
        Path contextDir = rootDir.resolve(getContextHeader(info));
        Files.createDirectories(contextDir);

        return contextDir;
    }

    private static void printAutomatedMessage(PrintStream stream) {
        stream.println("// This file is auto-generated. Do not edit.");
        stream.println();
    }

    private static void printSharedIndexPage(
        Path sharedDir,
        Map<String, String> javaNamesToDisplayNames,
        List<Object> staticInfos,
        List<PainlessContextClassInfo> classInfos
    ) throws IOException {

        Path sharedIndexPath = sharedDir.resolve("index.asciidoc");

        try (
            PrintStream sharedIndexStream = new PrintStream(
                Files.newOutputStream(sharedIndexPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                false,
                StandardCharsets.UTF_8
            )
        ) {

            printAutomatedMessage(sharedIndexStream);

            sharedIndexStream.println("[[" + SHARED_HEADER + "]]");
            sharedIndexStream.println("=== " + SHARED_NAME + " API");
            sharedIndexStream.println();
            sharedIndexStream.println("The following API is available in all contexts.");

            printIndex(sharedIndexStream, SHARED_HEADER, javaNamesToDisplayNames, staticInfos, classInfos);
        }
    }

    private static void printContextIndexPage(
        Path contextDir,
        Map<String, String> javaNamesToDisplayNames,
        PainlessContextInfo contextInfo,
        List<Object> staticInfos,
        List<PainlessContextClassInfo> classInfos
    ) throws IOException {

        Path contextIndexPath = contextDir.resolve("index.asciidoc");

        try (
            PrintStream contextIndexStream = new PrintStream(
                Files.newOutputStream(contextIndexPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                false,
                StandardCharsets.UTF_8
            )
        ) {

            printAutomatedMessage(contextIndexStream);

            contextIndexStream.println("[[" + getContextHeader(contextInfo) + "]]");
            contextIndexStream.println("=== " + getContextName(contextInfo) + " API");
            contextIndexStream.println();
            contextIndexStream.println("The following specialized API is available in the " + getContextName(contextInfo) + " context.");
            contextIndexStream.println();
            contextIndexStream.println(
                "* See the <<" + SHARED_HEADER + ", " + SHARED_NAME + " API>> for further API available in all contexts."
            );

            printIndex(contextIndexStream, getContextHeader(contextInfo), javaNamesToDisplayNames, staticInfos, classInfos);
        }
    }

    private static void printIndex(
        PrintStream indexStream,
        String contextHeader,
        Map<String, String> javaNamesToDisplayNames,
        List<Object> staticInfos,
        List<PainlessContextClassInfo> classInfos
    ) {

        String currentPackageName = null;

        if (staticInfos.isEmpty() == false) {
            indexStream.println();
            indexStream.println("==== Static Methods");
            indexStream.println(
                "The following methods are directly callable without a class/instance qualifier. "
                    + "Note parameters denoted by a (*) are treated as read-only values."
            );
            indexStream.println();

            for (Object staticInfo : staticInfos) {
                if (staticInfo instanceof PainlessContextMethodInfo) {
                    printMethod(indexStream, javaNamesToDisplayNames, false, (PainlessContextMethodInfo) staticInfo);
                } else if (staticInfo instanceof PainlessContextClassBindingInfo) {
                    printClassBinding(indexStream, javaNamesToDisplayNames, (PainlessContextClassBindingInfo) staticInfo);
                } else if (staticInfo instanceof PainlessContextInstanceBindingInfo) {
                    printInstanceBinding(indexStream, javaNamesToDisplayNames, (PainlessContextInstanceBindingInfo) staticInfo);
                } else {
                    throw new IllegalArgumentException("unexpected static info type");
                }
            }
        }

        if (classInfos.isEmpty() == false) {
            indexStream.println();
            indexStream.println("==== Classes By Package");
            indexStream.println(
                "The following classes are available grouped by their respective packages. Click on a class "
                    + "to view details about the available methods and fields."
            );
            indexStream.println();

            for (PainlessContextClassInfo classInfo : classInfos) {
                String classPackageName = classInfo.getName().substring(0, classInfo.getName().lastIndexOf('.'));

                if (classPackageName.equals(currentPackageName) == false) {
                    currentPackageName = classPackageName;

                    indexStream.println();
                    indexStream.println("==== " + currentPackageName);
                    indexStream.println(
                        "<<"
                            + getPackageHeader(contextHeader, currentPackageName)
                            + ", "
                            + "Expand details for "
                            + currentPackageName
                            + ">>"
                    );
                    indexStream.println();
                }

                String className = ContextGeneratorCommon.getType(javaNamesToDisplayNames, classInfo.getName());
                indexStream.println("* <<" + getClassHeader(contextHeader, className) + ", " + className + ">>");
            }
        }

        indexStream.println();
        indexStream.println("include::packages.asciidoc[]");
        indexStream.println();
    }

    private static void printSharedPackagesPages(
        Path sharedDir,
        Map<String, String> javaNamesToDisplayNames,
        List<PainlessContextClassInfo> classInfos
    ) throws IOException {

        Path sharedClassesPath = sharedDir.resolve("packages.asciidoc");

        try (
            PrintStream sharedPackagesStream = new PrintStream(
                Files.newOutputStream(sharedClassesPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                false,
                StandardCharsets.UTF_8
            )
        ) {

            printAutomatedMessage(sharedPackagesStream);
            printPackages(sharedPackagesStream, SHARED_NAME, SHARED_HEADER, javaNamesToDisplayNames, Collections.emptySet(), classInfos);
        }
    }

    private static void printContextPackagesPages(
        Path contextDir,
        Map<String, String> javaNamesToDisplayNames,
        Set<PainlessContextClassInfo> excludes,
        PainlessContextInfo contextInfo,
        List<PainlessContextClassInfo> classInfos
    ) throws IOException {

        Path contextPackagesPath = contextDir.resolve("packages.asciidoc");

        try (
            PrintStream contextPackagesStream = new PrintStream(
                Files.newOutputStream(contextPackagesPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                false,
                StandardCharsets.UTF_8
            )
        ) {

            printAutomatedMessage(contextPackagesStream);
            printPackages(
                contextPackagesStream,
                getContextName(contextInfo),
                getContextHeader(contextInfo),
                javaNamesToDisplayNames,
                excludes,
                classInfos
            );
        }
    }

    private static void printPackages(
        PrintStream packagesStream,
        String contextName,
        String contextHeader,
        Map<String, String> javaNamesToDisplayNames,
        Set<PainlessContextClassInfo> excludes,
        List<PainlessContextClassInfo> classInfos
    ) {

        String currentPackageName = null;

        for (PainlessContextClassInfo classInfo : classInfos) {
            if (excludes.contains(classInfo)) {
                continue;
            }

            String classPackageName = classInfo.getName().substring(0, classInfo.getName().lastIndexOf('.'));

            if (classPackageName.equals(currentPackageName) == false) {
                currentPackageName = classPackageName;

                packagesStream.println();
                packagesStream.println("[role=\"exclude\",id=\"" + getPackageHeader(contextHeader, currentPackageName) + "\"]");
                packagesStream.println("=== " + contextName + " API for package " + currentPackageName);
                packagesStream.println(
                    "See the <<" + contextHeader + ", " + contextName + " API>> " + "for a high-level overview of all packages and classes."
                );
            }

            String className = ContextGeneratorCommon.getType(javaNamesToDisplayNames, classInfo.getName());
            packagesStream.println();
            packagesStream.println("[[" + getClassHeader(contextHeader, className) + "]]");
            packagesStream.println("==== " + className + "");

            for (PainlessContextFieldInfo fieldInfo : classInfo.getStaticFields()) {
                printField(packagesStream, javaNamesToDisplayNames, true, fieldInfo);
            }

            for (PainlessContextMethodInfo methodInfo : classInfo.getStaticMethods()) {
                printMethod(packagesStream, javaNamesToDisplayNames, true, methodInfo);
            }

            for (PainlessContextFieldInfo fieldInfo : classInfo.getFields()) {
                printField(packagesStream, javaNamesToDisplayNames, false, fieldInfo);
            }

            for (PainlessContextConstructorInfo constructorInfo : classInfo.getConstructors()) {
                printConstructor(packagesStream, javaNamesToDisplayNames, className, constructorInfo);
            }

            for (PainlessContextMethodInfo methodInfo : classInfo.getMethods()) {
                printMethod(packagesStream, javaNamesToDisplayNames, false, methodInfo);
            }

            packagesStream.println();
        }

        packagesStream.println();
    }

    private static void printRootIndexPage(Path rootDir, List<PainlessContextInfo> contextInfos, Set<PainlessContextInfo> isSpecialized)
        throws IOException {
        Path rootIndexPath = rootDir.resolve("index.asciidoc");

        try (
            PrintStream rootIndexStream = new PrintStream(
                Files.newOutputStream(rootIndexPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                false,
                StandardCharsets.UTF_8
            )
        ) {

            printAutomatedMessage(rootIndexStream);

            rootIndexStream.println("[cols=\"<3,^3,^3\"]");
            rootIndexStream.println("|====");

            for (PainlessContextInfo contextInfo : contextInfos) {
                String contextName = getContextName(contextInfo);
                String contextHeader = getContextHeader(contextInfo);

                rootIndexStream.print("|" + contextName + " ");
                rootIndexStream.print("| <<" + SHARED_HEADER + ", " + SHARED_NAME + " API>> ");

                if (isSpecialized.contains(contextInfo)) {
                    rootIndexStream.println("| <<" + contextHeader + ", Specialized API>>");
                } else {
                    rootIndexStream.println("| ");
                }
            }

            rootIndexStream.println("|====");
            rootIndexStream.println();

            rootIndexStream.println("include::" + SHARED_HEADER + "/index.asciidoc[]");

            for (PainlessContextInfo contextInfo : contextInfos) {
                if (isSpecialized.contains(contextInfo)) {
                    rootIndexStream.println("include::" + getContextHeader(contextInfo) + "/index.asciidoc[]");
                }
            }
        }
    }

    private static void printConstructor(
        PrintStream stream,
        Map<String, String> javaNamesToDisplayNames,
        String className,
        PainlessContextConstructorInfo constructorInfo
    ) {

        stream.print("* ");

        if (constructorInfo.getDeclaring().startsWith("java.")) {
            stream.print(getConstructorJavaDocLink(constructorInfo) + "[" + className + "]");
        } else {
            stream.print(className);
        }

        stream.print("(");

        for (int parameterIndex = 0; parameterIndex < constructorInfo.getParameters().size(); ++parameterIndex) {

            stream.print(ContextGeneratorCommon.getType(javaNamesToDisplayNames, constructorInfo.getParameters().get(parameterIndex)));

            if (parameterIndex + 1 < constructorInfo.getParameters().size()) {
                stream.print(", ");
            }
        }

        stream.println(")");
    }

    private static void printMethod(
        PrintStream stream,
        Map<String, String> javaNamesToDisplayNames,
        boolean isStatic,
        PainlessContextMethodInfo methodInfo
    ) {

        stream.print("* " + (isStatic ? "static " : ""));
        stream.print(ContextGeneratorCommon.getType(javaNamesToDisplayNames, methodInfo.getRtn()) + " ");

        if (methodInfo.getDeclaring().startsWith("java.")) {
            stream.print(getMethodJavaDocLink(methodInfo) + "[" + methodInfo.getName() + "]");
        } else {
            stream.print(methodInfo.getName());
        }

        stream.print("(");

        for (int parameterIndex = 0; parameterIndex < methodInfo.getParameters().size(); ++parameterIndex) {

            stream.print(ContextGeneratorCommon.getType(javaNamesToDisplayNames, methodInfo.getParameters().get(parameterIndex)));

            if (parameterIndex + 1 < methodInfo.getParameters().size()) {
                stream.print(", ");
            }
        }

        stream.println(")");
    }

    private static void printClassBinding(
        PrintStream stream,
        Map<String, String> javaNamesToDisplayNames,
        PainlessContextClassBindingInfo classBindingInfo
    ) {

        stream.print(
            "* "
                + ContextGeneratorCommon.getType(javaNamesToDisplayNames, classBindingInfo.getRtn())
                + " "
                + classBindingInfo.getName()
                + "("
        );

        for (int parameterIndex = 0; parameterIndex < classBindingInfo.getParameters().size(); ++parameterIndex) {
            // temporary fix to not print org.elasticsearch.script.ScoreScript parameter until
            // class instance bindings are created and the information is appropriately added to the context info classes
            if ("org.elasticsearch.script.ScoreScript".equals(
                ContextGeneratorCommon.getType(javaNamesToDisplayNames, classBindingInfo.getParameters().get(parameterIndex))
            )) {
                continue;
            }

            stream.print(ContextGeneratorCommon.getType(javaNamesToDisplayNames, classBindingInfo.getParameters().get(parameterIndex)));

            if (parameterIndex < classBindingInfo.getReadOnly()) {
                stream.print(" *");
            }

            if (parameterIndex + 1 < classBindingInfo.getParameters().size()) {
                stream.print(", ");
            }
        }

        stream.println(")");
    }

    private static void printInstanceBinding(
        PrintStream stream,
        Map<String, String> javaNamesToDisplayNames,
        PainlessContextInstanceBindingInfo instanceBindingInfo
    ) {

        stream.print(
            "* "
                + ContextGeneratorCommon.getType(javaNamesToDisplayNames, instanceBindingInfo.getRtn())
                + " "
                + instanceBindingInfo.getName()
                + "("
        );

        for (int parameterIndex = 0; parameterIndex < instanceBindingInfo.getParameters().size(); ++parameterIndex) {
            stream.print(ContextGeneratorCommon.getType(javaNamesToDisplayNames, instanceBindingInfo.getParameters().get(parameterIndex)));

            if (parameterIndex + 1 < instanceBindingInfo.getParameters().size()) {
                stream.print(", ");
            }
        }

        stream.println(")");
    }

    private static void printField(
        PrintStream stream,
        Map<String, String> javaNamesToDisplayNames,
        boolean isStatic,
        PainlessContextFieldInfo fieldInfo
    ) {

        stream.print("* " + (isStatic ? "static " : ""));
        stream.print(ContextGeneratorCommon.getType(javaNamesToDisplayNames, fieldInfo.getType()) + " ");

        if (fieldInfo.getDeclaring().startsWith("java.")) {
            stream.println(getFieldJavaDocLink(fieldInfo) + "[" + fieldInfo.getName() + "]");
        } else {
            stream.println(fieldInfo.getName());
        }
    }

    private static String getFieldJavaDocLink(PainlessContextFieldInfo fieldInfo) {
        return "{java11-javadoc}/java.base/" + fieldInfo.getDeclaring().replace('.', '/') + ".html#" + fieldInfo.getName();
    }

    private static String getConstructorJavaDocLink(PainlessContextConstructorInfo constructorInfo) {
        StringBuilder javaDocLink = new StringBuilder();

        javaDocLink.append("{java11-javadoc}/java.base/");
        javaDocLink.append(constructorInfo.getDeclaring().replace('.', '/'));
        javaDocLink.append(".html#<init>(");

        return collectParameters(javaDocLink, constructorInfo.getParameters());
    }

    private static String getMethodJavaDocLink(PainlessContextMethodInfo methodInfo) {
        StringBuilder javaDocLink = new StringBuilder();

        javaDocLink.append("{java11-javadoc}/java.base/");
        javaDocLink.append(methodInfo.getDeclaring().replace('.', '/'));
        javaDocLink.append(".html#");
        javaDocLink.append(methodInfo.getName());
        javaDocLink.append("(");

        return collectParameters(javaDocLink, methodInfo.getParameters());
    }

    private static String collectParameters(StringBuilder javaDocLink, List<String> parameters) {
        for (int parameterIndex = 0; parameterIndex < parameters.size(); ++parameterIndex) {

            javaDocLink.append(getLinkType(parameters.get(parameterIndex)));

            if (parameterIndex + 1 < parameters.size()) {
                javaDocLink.append(",");
            }
        }

        javaDocLink.append(")");

        return javaDocLink.toString();
    }

    private static String getLinkType(String javaType) {
        int arrayDimensions = 0;

        while (javaType.charAt(arrayDimensions) == '[') {
            ++arrayDimensions;
        }

        if (arrayDimensions > 0) {
            if (javaType.charAt(javaType.length() - 1) == ';') {
                javaType = javaType.substring(arrayDimensions + 1, javaType.length() - 1);
            } else {
                javaType = javaType.substring(arrayDimensions);
            }
        }

        if ("Z".equals(javaType) || "boolean".equals(javaType)) {
            javaType = "boolean";
        } else if ("V".equals(javaType) || "void".equals(javaType)) {
            javaType = "void";
        } else if ("B".equals(javaType) || "byte".equals(javaType)) {
            javaType = "byte";
        } else if ("S".equals(javaType) || "short".equals(javaType)) {
            javaType = "short";
        } else if ("C".equals(javaType) || "char".equals(javaType)) {
            javaType = "char";
        } else if ("I".equals(javaType) || "int".equals(javaType)) {
            javaType = "int";
        } else if ("J".equals(javaType) || "long".equals(javaType)) {
            javaType = "long";
        } else if ("F".equals(javaType) || "float".equals(javaType)) {
            javaType = "float";
        } else if ("D".equals(javaType) || "double".equals(javaType)) {
            javaType = "double";
        } else if ("org.elasticsearch.painless.lookup.def".equals(javaType)) {
            javaType = "java.lang.Object";
        }

        while (arrayDimensions-- > 0) {
            javaType += "%5B%5D";
        }

        return javaType;
    }

    private static String getContextHeader(PainlessContextInfo contextInfo) {
        return "painless-api-reference-" + contextInfo.getName().replace(" ", "-").replace("_", "-");
    }

    private static String getPackageHeader(String contextHeader, String packageName) {
        return contextHeader + "-" + packageName.replace('.', '-');
    }

    private static String getClassHeader(String contextHeader, String className) {
        return contextHeader + "-" + className.replace('.', '-');
    }

    private static String getContextName(PainlessContextInfo contextInfo) {
        String[] split = contextInfo.getName().split("[_-]");
        StringBuilder contextNameBuilder = new StringBuilder();

        for (String part : split) {
            contextNameBuilder.append(Character.toUpperCase(part.charAt(0)));
            contextNameBuilder.append(part.substring(1));
            contextNameBuilder.append(' ');
        }

        return contextNameBuilder.substring(0, contextNameBuilder.length() - 1);
    }

    private static List<Object> sortStaticInfos(Set<Object> staticExcludes, List<Object> staticInfos) {
        staticInfos = new ArrayList<>(staticInfos);
        staticInfos.removeIf(staticExcludes::contains);

        staticInfos.sort(Comparator.comparing(si -> {
            String sv;
            if (si instanceof PainlessContextMethodInfo) {
                sv = ((PainlessContextMethodInfo) si).getSortValue();
            } else if (si instanceof PainlessContextClassBindingInfo) {
                sv = ((PainlessContextClassBindingInfo) si).getSortValue();
            } else if (si instanceof PainlessContextInstanceBindingInfo) {
                sv = ((PainlessContextInstanceBindingInfo) si).getSortValue();
            } else {
                throw new IllegalArgumentException("unexpected static info type");
            }
            return sv;
        }));

        return staticInfos;
    }

    private static List<PainlessContextClassInfo> sortClassInfos(
        Set<PainlessContextClassInfo> classExcludes,
        List<PainlessContextClassInfo> classInfos
    ) {
        classInfos = new ArrayList<>(classInfos);
        classInfos.removeIf(v -> ContextGeneratorCommon.isExcludedClassInfo(v) || classExcludes.contains(v));
        return ContextGeneratorCommon.sortFilteredClassInfos(classInfos);
    }

    private static Map<String, String> getDisplayNames(List<PainlessContextClassInfo> classInfos) {
        Map<String, String> javaNamesToDisplayNames = new HashMap<>();

        for (PainlessContextClassInfo classInfo : classInfos) {
            String className = classInfo.getName();

            if (classInfo.isImported()) {
                javaNamesToDisplayNames.put(className, className.substring(className.lastIndexOf('.') + 1).replace('$', '.'));
            } else {
                javaNamesToDisplayNames.put(className, className.replace('$', '.'));
            }
        }

        return javaNamesToDisplayNames;
    }

    private ContextDocGenerator() {}
}
