/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.jline.utils.AttributedStringBuilder;

import static org.jline.utils.AttributedStyle.BOLD;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.WHITE;

abstract class ResponseToString {

    static AttributedStringBuilder toAnsi(Response response) {
        AttributedStringBuilder sb = new AttributedStringBuilder();

        if (response instanceof CommandResponse) {
            CommandResponse cmd = (CommandResponse) response;
            if (cmd.data != null) {
                String data = cmd.data.toString();
                if (data.startsWith("digraph ")) {
                    displayGraphviz(data);
                }
                else {
                    sb.append(data, DEFAULT.foreground(WHITE));
                }
            }
        }
        else if (response instanceof ExceptionResponse) {
            ExceptionResponse ex = (ExceptionResponse) response;
            sb.append(ex.message, BOLD.foreground(CYAN));
        }
        else if (response instanceof InfoResponse) {
            InfoResponse info = (InfoResponse) response;
            sb.append("Node:", DEFAULT.foreground(BRIGHT));
            sb.append(info.node, DEFAULT.foreground(WHITE));
            sb.append(" Cluster:", DEFAULT.foreground(BRIGHT));
            sb.append(info.cluster, DEFAULT.foreground(WHITE));
            sb.append(" Version:", DEFAULT.foreground(BRIGHT));
            sb.append(info.versionString, DEFAULT.foreground(WHITE));
        }
        else if (response instanceof ErrorResponse) {
            ErrorResponse error = (ErrorResponse) response;
            sb.append("Server error:", BOLD.foreground(RED));
            sb.append(error.message, DEFAULT.italic().foreground(RED));
        }
        else {
            sb.append("Invalid response received from server...", BOLD.foreground(RED));
        }
        
        return sb;
    }


    private static void displayGraphviz(String str) {
        try {
            // save the content to a temp file
            Path dotTempFile = Files.createTempFile("sql-gv", ".dot2img");
            Files.write(dotTempFile, str.getBytes(StandardCharsets.UTF_8));
            // run graphviz on it (dot needs to be on the file path)
            Desktop desktop = Desktop.getDesktop();
            File f = dotTempFile.toFile();
            desktop.open(f);
            f.deleteOnExit();

        } catch (IOException ex) {
            // nope
        }
    }
}
