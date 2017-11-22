/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryResponse;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.jline.utils.AttributedStringBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.WHITE;

abstract class ResponseToString {

    static AttributedStringBuilder toAnsi(Response response) {
        AttributedStringBuilder sb = new AttributedStringBuilder();

        switch ((ResponseType) response.responseType()) {
        case QUERY_INIT:
        case QUERY_PAGE:
            QueryResponse cmd = (QueryResponse) response;
            if (cmd.data != null) {
                String data = cmd.data.toString();
                if (data.startsWith("digraph ")) {
                    sb.append(handleGraphviz(data), DEFAULT.foreground(WHITE));
                }
                else {
                    sb.append(data, DEFAULT.foreground(WHITE));
                }
            }
            return sb;
        case INFO:
            InfoResponse info = (InfoResponse) response;
            sb.append("Node:", DEFAULT.foreground(BRIGHT));
            sb.append(info.node, DEFAULT.foreground(WHITE));
            sb.append(" Cluster:", DEFAULT.foreground(BRIGHT));
            sb.append(info.cluster, DEFAULT.foreground(WHITE));
            sb.append(" Version:", DEFAULT.foreground(BRIGHT));
            sb.append(info.versionString, DEFAULT.foreground(WHITE));
            return sb;
        default:
            throw new IllegalArgumentException("Unsupported response: " + response);
        }
    }

    private static String handleGraphviz(String str) {
        try {
            // save the content to a temp file
            Path dotTempFile = Files.createTempFile(Paths.get("."), "sql-gv", ".dot");
            Files.write(dotTempFile, str.getBytes(StandardCharsets.UTF_8));
            // run graphviz on it (dot needs to be on the file path)
            //Desktop desktop = Desktop.getDesktop();
            //File f = dotTempFile.toFile();
            //desktop.open(f);
            //f.deleteOnExit();
            return "Saved graph file at " + dotTempFile;

        } catch (IOException ex) {
            return "Cannot save graph file; " + ex.getMessage();
        }
    }
}
