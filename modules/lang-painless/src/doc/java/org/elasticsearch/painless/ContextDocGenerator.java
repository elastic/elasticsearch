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

package org.elasticsearch.painless;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.painless.action.PainlessContextClassInfo;
import org.elasticsearch.painless.action.PainlessContextConstructorInfo;
import org.elasticsearch.painless.action.PainlessContextFieldInfo;
import org.elasticsearch.painless.action.PainlessContextInfo;
import org.elasticsearch.painless.action.PainlessContextMethodInfo;

import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public final class ContextDocGenerator {

    public static void main(String[] args) throws Exception {
        URLConnection getContextList = new URL(
                "http://" + System.getProperty("cluster.uri") + "/_scripts/painless/_context").openConnection();
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, getContextList.getInputStream());
        parser.nextToken();
        parser.nextToken();
        @SuppressWarnings("unchecked")
        List<String> contexts = (List<String>)(Object)parser.list();
        parser.close();
        ((HttpURLConnection)getContextList).disconnect();

        List<PainlessContextInfo> painlessContextInfoList = new ArrayList<>();
        for (String context : contexts) {
            URLConnection getContextInfo = new URL(
                    "http://" + System.getProperty("cluster.uri") + "/_scripts/painless/_context?context=" + context).openConnection();
            parser = JsonXContent.jsonXContent.createParser(null, null, getContextInfo.getInputStream());
            painlessContextInfoList.add(PainlessContextInfo.fromXContent(parser));
            ((HttpURLConnection)getContextInfo).disconnect();
        }

        Path apiRootPath = PathUtils.get("../../docs/painless/painless-api-reference");
        IOUtils.rm(apiRootPath);
        Files.createDirectories(apiRootPath);
        Path apiIndexPath = apiRootPath.resolve("index.asciidoc");
        List<String> contextApiHeaders = new ArrayList<>();

        try (PrintStream indexStream = new PrintStream(
                Files.newOutputStream(apiIndexPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                false, StandardCharsets.UTF_8.name())) {

            for (PainlessContextInfo painlessContextInfo : painlessContextInfoList) {
                String contextApiHeader = "painless-api-reference-" + painlessContextInfo.name.replace(" ", "").replace("_", "-");
                contextApiHeaders.add(contextApiHeader);
                String[] split = painlessContextInfo.name.split("[_-]");
                StringBuilder contextNameBuilder = new StringBuilder();

                for (String part : split) {
                    contextNameBuilder.append(Character.toUpperCase(part.charAt(0)));
                    contextNameBuilder.append(part.substring(1));
                    contextNameBuilder.append(' ');
                }

                String contextName = contextNameBuilder.substring(0, contextNameBuilder.length() - 1);
                indexStream.println("* <<" + contextApiHeader + ", " + contextName  + ">>");

                Path contextApiRootPath = apiRootPath.resolve(contextApiHeader);
                Files.createDirectories(contextApiRootPath);
                Path contextApiIndexPath = contextApiRootPath.resolve("index.asciidoc");

                try (PrintStream contextApiIndexStream = new PrintStream(
                        Files.newOutputStream(contextApiIndexPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                        false, StandardCharsets.UTF_8.name())) {

                    contextApiIndexStream.println("[[" + contextApiHeader  + "]]");
                    contextApiIndexStream.println("=== " + contextName + " API");

                    List<PainlessContextClassInfo> painlessContextClassInfos = new ArrayList<>(painlessContextInfo.classes);
                    painlessContextClassInfos.removeIf(v -> "void".equals(v.name) || "boolean".equals(v.name) || "byte".equals(v.name) ||
                            "short".equals(v.name) || "char".equals(v.name) || "int".equals(v.name) || "long".equals(v.name) ||
                            "float".equals(v.name) || "double".equals(v.name) || "org.elasticsearch.painless.lookup.def".equals(v.name));

                    painlessContextClassInfos.sort((c1, c2) -> {
                        String n1 = c1.name;
                        String n2 = c2.name;
                        boolean i1 = c1.imported;
                        boolean i2 = c2.imported;

                        String p1 = n1.substring(0, n1.lastIndexOf('.'));
                        String p2 = n2.substring(0, n2.lastIndexOf('.'));

                        int compare = p1.compareTo(p2);

                        if (compare == 0) {
                            if (i1 && i2) {
                                compare = n1.substring(n1.lastIndexOf('.') + 1).compareTo(n2.substring(n2.lastIndexOf('.') + 1));
                            } else if (i1 == false && i2 == false) {
                                compare = n1.compareTo(n2);
                            } else {
                                compare = Boolean.compare(i1, i2) * -1;
                            }
                        }

                        return compare;
                    });

                    Map<String, String> javaClassNamesToPainlessClassNames = new HashMap<>();
                    SortedMap<String, List<PainlessContextClassInfo>> packagesToPainlessContextClassInfos = new TreeMap<>();
                    String currentPackageName = null;

                    for (PainlessContextClassInfo painlessContextClassInfo : painlessContextClassInfos) {
                        String className = painlessContextClassInfo.name;

                        if (painlessContextClassInfo.imported) {
                            javaClassNamesToPainlessClassNames.put(className,
                                    className.substring(className.lastIndexOf('.') + 1).replace('$', '.'));
                        } else {
                            javaClassNamesToPainlessClassNames.put(className, className.replace('$', '.'));
                        }

                        String classPackageName = className.substring(0, className.lastIndexOf('.'));

                        if (classPackageName.equals(currentPackageName) == false) {
                            currentPackageName = classPackageName;
                            packagesToPainlessContextClassInfos.put(currentPackageName, new ArrayList<>());
                        }

                        packagesToPainlessContextClassInfos.get(currentPackageName).add(painlessContextClassInfo);
                    }

                    List<String> contextApiPackageHeaders = new ArrayList<>();

                    for (Map.Entry<String, List<PainlessContextClassInfo>> packageToPainlessContextClassInfo :
                            packagesToPainlessContextClassInfos.entrySet()) {
                        String packageName = packageToPainlessContextClassInfo.getKey();

                        String contextApiPackageHeader = contextApiHeader + "-" + packageName.replace('.', '-');
                        contextApiPackageHeaders.add(contextApiPackageHeader);
                        Path contextApiPackagePath = contextApiRootPath.resolve(contextApiPackageHeader + ".asciidoc");

                        contextApiIndexStream.println();
                        contextApiIndexStream.println("==== " + packageName);
                        contextApiIndexStream.println("<<" + contextApiPackageHeader + ", Expand " + packageName + ">>");
                        contextApiIndexStream.println();

                        try (PrintStream contextApiPackageStream = new PrintStream(
                                Files.newOutputStream(contextApiPackagePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                                false, StandardCharsets.UTF_8.name())) {

                            contextApiPackageStream.println("[role=\"exclude\",id=\"" + contextApiPackageHeader + "\"]");
                            contextApiPackageStream.println("=== " + packageName);

                            for (PainlessContextClassInfo painlessContextClassInfo : packageToPainlessContextClassInfo.getValue()) {
                                String className = javaClassNamesToPainlessClassNames.get(painlessContextClassInfo.name);
                                String classHeader = contextApiPackageHeader + "-" + className;

                                contextApiPackageStream.println();
                                contextApiPackageStream.println("[[" + classHeader + "]]");
                                contextApiPackageStream.println("==== " + className);

                                for (PainlessContextFieldInfo painlessContextFieldInfo : painlessContextClassInfo.staticFields) {
                                    printField(contextApiPackageStream, javaClassNamesToPainlessClassNames,
                                            true, painlessContextFieldInfo);
                                }

                                for (PainlessContextMethodInfo painlessContextMethodInfo : painlessContextClassInfo.staticMethods) {
                                    printMethod(contextApiPackageStream, javaClassNamesToPainlessClassNames,
                                            true, painlessContextMethodInfo);
                                }

                                for (PainlessContextFieldInfo painlessContextFieldInfo : painlessContextClassInfo.fields) {
                                    printField(contextApiPackageStream, javaClassNamesToPainlessClassNames,
                                            false, painlessContextFieldInfo);
                                }

                                for (PainlessContextConstructorInfo painlessContextConstructorInfo :
                                        painlessContextClassInfo.constructors) {

                                    printConstructor(contextApiPackageStream, javaClassNamesToPainlessClassNames,
                                            className, painlessContextConstructorInfo);
                                }

                                for (PainlessContextMethodInfo painlessContextMethodInfo : painlessContextClassInfo.methods) {
                                    printMethod(contextApiPackageStream, javaClassNamesToPainlessClassNames,
                                            false, painlessContextMethodInfo);
                                }

                                contextApiPackageStream.println();

                                contextApiIndexStream.println("* <<" + classHeader + ", " + className + ">>");
                            }

                            contextApiPackageStream.println();
                        }

                        contextApiIndexStream.println();
                    }

                    contextApiIndexStream.println();

                    for (String contextPackageApiHeader : contextApiPackageHeaders) {
                        contextApiIndexStream.println("include::" + contextPackageApiHeader + ".asciidoc[]");
                    }
                }
            }

            for (String contextApiHeader : contextApiHeaders) {
                indexStream.println();
                indexStream.println("include::" + contextApiHeader + "/index.asciidoc[]");
            }
        }
    }

    private static void printField(
            PrintStream stream, Map<String, String> javaClassNamesToPainlessClassNames,
            boolean isStatic, PainlessContextFieldInfo painlessContextFieldInfo) {

        stream.print("* " + (isStatic ? "static " : ""));
        stream.print(getType(javaClassNamesToPainlessClassNames, painlessContextFieldInfo.type) + " ");

        if (painlessContextFieldInfo.declaring.startsWith("java.")) {
            stream.println(getFieldJavaDocLink(painlessContextFieldInfo) + "[" + painlessContextFieldInfo.name + "]");
        } else {
            stream.println(painlessContextFieldInfo.name);
        }
    }

    private static void printConstructor(
            PrintStream stream, Map<String, String> javaClassNamesToPainlessClassNames,
            String className, PainlessContextConstructorInfo painlessContextConstructorInfo) {

        stream.print("* ");

        if (painlessContextConstructorInfo.declaring.startsWith("java.")) {
            stream.print(getConstructorJavaDocLink(painlessContextConstructorInfo) + "[" + className + "]");
        } else {
            stream.print(className);
        }

        stream.print("(");

        for (int parameterIndex = 0;
             parameterIndex < painlessContextConstructorInfo.parameters.size();
             ++parameterIndex) {

            stream.print(getType(javaClassNamesToPainlessClassNames, painlessContextConstructorInfo.parameters.get(parameterIndex)));

            if (parameterIndex + 1 < painlessContextConstructorInfo.parameters.size()) {
                stream.print(", ");
            }
        }

        stream.println(")");
    }

    private static void printMethod(
            PrintStream stream, Map<String, String> javaClassNamesToPainlessClassNames,
            boolean isStatic, PainlessContextMethodInfo painlessContextMethodInfo) {

        stream.print("* " + (isStatic ? "static " : ""));
        stream.print(getType(javaClassNamesToPainlessClassNames, painlessContextMethodInfo.rtn) + " ");

        if (painlessContextMethodInfo.declaring.startsWith("java.")) {
            stream.print(getMethodJavaDocLink(painlessContextMethodInfo) + "[" + painlessContextMethodInfo.name + "]");
        } else {
            stream.print(painlessContextMethodInfo.name);
        }

        stream.print("(");

        for (int parameterIndex = 0;
             parameterIndex < painlessContextMethodInfo.parameters.size();
             ++parameterIndex) {

            stream.print(getType(javaClassNamesToPainlessClassNames, painlessContextMethodInfo.parameters.get(parameterIndex)));

            if (parameterIndex + 1 < painlessContextMethodInfo.parameters.size()) {
                stream.print(", ");
            }
        }

        stream.println(")");
    }

    private static String getType(Map<String, String> javaClassNamesToPainlessClassNames, String javaType) {
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
            javaType = "def";
        } else {
            javaType = javaClassNamesToPainlessClassNames.get(javaType);
        }

        while (arrayDimensions-- > 0) {
            javaType += "[]";
        }

        return javaType;
    }

    private static String getFieldJavaDocLink(PainlessContextFieldInfo painlessContextFieldInfo) {
        StringBuilder javaDocLink = new StringBuilder();

        javaDocLink.append("{java11-javadoc}/java.base/");
        javaDocLink.append(painlessContextFieldInfo.declaring.replace('.', '/'));
        javaDocLink.append(".html#");
        javaDocLink.append(painlessContextFieldInfo.name);

        return javaDocLink.toString();
    }

    private static String getConstructorJavaDocLink(PainlessContextConstructorInfo painlessContextConstructorInfo) {
        StringBuilder javaDocLink = new StringBuilder();

        javaDocLink.append("{java11-javadoc}/java.base/");
        javaDocLink.append(painlessContextConstructorInfo.declaring.replace('.', '/'));
        javaDocLink.append(".html#<init>(");

        for (int parameterIndex = 0;
             parameterIndex < painlessContextConstructorInfo.parameters.size();
             ++parameterIndex) {

            javaDocLink.append(getLinkType(painlessContextConstructorInfo.parameters.get(parameterIndex)));

            if (parameterIndex + 1 < painlessContextConstructorInfo.parameters.size()) {
                javaDocLink.append(",");
            }
        }

        javaDocLink.append(")");

        return javaDocLink.toString();
    }

    private static String getMethodJavaDocLink(PainlessContextMethodInfo painlessContextMethodInfo) {
        StringBuilder javaDocLink = new StringBuilder();

        javaDocLink.append("{java11-javadoc}/java.base/");
        javaDocLink.append(painlessContextMethodInfo.declaring.replace('.', '/'));
        javaDocLink.append(".html#");
        javaDocLink.append(painlessContextMethodInfo.name);
        javaDocLink.append("(");

        for (int parameterIndex = 0;
             parameterIndex < painlessContextMethodInfo.parameters.size();
             ++parameterIndex) {

            javaDocLink.append(getLinkType(painlessContextMethodInfo.parameters.get(parameterIndex)));

            if (parameterIndex + 1 < painlessContextMethodInfo.parameters.size()) {
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

    private ContextDocGenerator() {

    }
}
