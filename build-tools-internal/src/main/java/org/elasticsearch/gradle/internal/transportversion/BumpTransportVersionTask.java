/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.transportversion;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BumpTransportVersionTask extends DefaultTask {

    @TaskAction
    public void run() throws IOException {
        Path transportVersionFile = Path.of("./server/src/main/java/org/elasticsearch/TransportVersion.java");
        String content = Files.readString(transportVersionFile);

        Pattern nextVersionPattern = Pattern.compile("// NEXT_VERSION ([\\d_]+)");
        Matcher matcher = nextVersionPattern.matcher(content);
        if (matcher.find()) {
            String version = matcher.group(1).replace("_", "");
            int nextVersion = Integer.valueOf(version);

            String newConstant = newConstantLine(nextVersion);
            String newVerComment = newNextVersionCommentLine(nextVersion + 1);

            String newCode = Stream.of(newConstant, newVerComment).collect(Collectors.joining(System.lineSeparator()));
            String contentWithNewLines = matcher.replaceAll(newCode);

            Pattern currentVersionPattern = Pattern.compile("public static final TransportVersion CURRENT = .*");
            Matcher currentVersionMatcher = currentVersionPattern.matcher(contentWithNewLines);
            if (currentVersionMatcher.find()) {
                String fileWithNewCurrent = currentVersionMatcher.replaceAll(
                    "public static final TransportVersion CURRENT = V_" + formatVersionNumber(nextVersion) + ";"
                );

                System.out.println(fileWithNewCurrent);
                Files.write(transportVersionFile, fileWithNewCurrent.getBytes(StandardCharsets.UTF_8));
            } else {
                System.out.println("error parsing CURRENT");
            }

        } else {
            System.out.println("error parsing next version");

        }

    }

    private static String newConstantLine(int version) {
        String newConstant = "public static final TransportVersion V_%s = new TransportVersion(%s, \"%s\");";
        String formattedVersion = formatVersionNumber(version);
        UUID uuid = UUID.randomUUID();

        return newConstant.formatted(formattedVersion, formattedVersion, uuid.toString());
    }

    private static String newNextVersionCommentLine(int version) {
        int newVer = version;
        return "    // NEXT_VERSION " + formatVersionNumber(newVer);
    }

    @NotNull
    private static String formatVersionNumber(int newVer) {
        return String.format("%,d", newVer).replace(',', '_');
    }

    public static void main(String[] args) throws IOException {
        new BumpTransportVersionTask().run();
    }
}
