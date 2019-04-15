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
import org.elasticsearch.painless.action.PainlessContextInfo;

import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class ContextDocGenerator {

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
                            "float".equals(v.name) || "double".equals(v.name) || "def".equals(v.name));

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

                    List<String> contextClassApiPaths = new ArrayList<>();
                    String currentPackageName = null;

                    for (PainlessContextClassInfo painlessContextClassInfo : painlessContextClassInfos) {
                        String className = painlessContextClassInfo.name;
                        String classPackageName = className.substring(0, className.lastIndexOf('.'));

                        if (classPackageName.equals(currentPackageName) == false) {
                            currentPackageName = classPackageName;
                            contextApiIndexStream.println();
                            contextApiIndexStream.println("==== " + currentPackageName);
                        }

                        if (painlessContextClassInfo.imported) {
                            className = className.substring(className.lastIndexOf('.') + 1).replace('$', '.');
                        } else {
                            className = className.replace('$', '.');
                        }

                        String contextClassApiHeader = contextApiHeader + "-" + className.replace('.', '-');
                        Path contextClassApiPath = contextApiRootPath.resolve(className.replace('.', '-') + ".asciidoc");
                        contextClassApiPaths.add(className.replace('.', '-') + ".asciidoc");

                        try (PrintStream contextClassApiStream = new PrintStream(
                                Files.newOutputStream(contextClassApiPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                                false, StandardCharsets.UTF_8.name())) {
                            contextClassApiStream.println("[[" + contextClassApiHeader + "]]");
                            contextClassApiStream.println("===== " + className);
                            contextClassApiStream.println("test");
                        }

                        contextApiIndexStream.println("* <<" + contextClassApiHeader + ", " + className + ">>");
                    }

                    contextApiIndexStream.println();

                    for (String contextClassApiPath : contextClassApiPaths) {
                        contextApiIndexStream.println("include::" + contextClassApiPath + "[]");
                    }
                }
            }

            for (String contextApiHeader : contextApiHeaders) {
                indexStream.println();
                indexStream.println("include::" + contextApiHeader + "/index.asciidoc[]");
            }
        }
    }
}
