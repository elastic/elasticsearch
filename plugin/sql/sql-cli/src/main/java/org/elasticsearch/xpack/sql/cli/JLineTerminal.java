/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.InfoCmp;

import java.io.IOException;

import static org.jline.utils.AttributedStyle.BOLD;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

/**
 * jline-based implementation of the terminal
 */
public class JLineTerminal implements CliTerminal {

    private Terminal terminal;
    private LineReader reader;

    /**
     * Build the terminal.
     * @param terminal the jLine terminal to work with
     * @param enableMatchBracket should jLine bounce the cursor to matching brackets?
     *      this is disabled in tests because it very difficult to predict and
     *      enabled in production because it is fairly nice.
     */
    public JLineTerminal(Terminal terminal, boolean enableMatchBracket) {
        this(terminal,
            LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(Completers.INSTANCE)
                .build(),
            enableMatchBracket);
    }

    /**
     * Constructor for tests.
     */
    JLineTerminal(Terminal terminal, LineReader reader, boolean enableMatchBracket) {
        this.terminal = terminal;
        this.reader = reader;
        if (false == enableMatchBracket) {
            reader.setVariable(LineReader.BLINK_MATCHING_PAREN, 0L);
        }
    }

    @Override
    public LineBuilder line() {
        return new LineBuilder();
    }

    @Override
    public void print(String text) {
        terminal.writer().print(text);
    }

    @Override
    public void println(String text) {
        terminal.writer().println(text);
    }

    @Override
    public void error(String type, String message) {
        AttributedStringBuilder sb = new AttributedStringBuilder();
        sb.append(type + " [", BOLD.foreground(RED));
        sb.append(message, DEFAULT.boldOff().italic().foreground(YELLOW));
        sb.append("]", BOLD.underlineOff().foreground(RED));
        terminal.writer().print(sb.toAnsi(terminal));
        terminal.flush();
    }

    @Override
    public void println() {
        terminal.writer().println();
    }

    @Override
    public void clear() {
        terminal.puts(InfoCmp.Capability.clear_screen);
    }

    @Override
    public void flush() {
        terminal.flush();
    }

    @Override
    public void printStackTrace(Exception ex) {
        ex.printStackTrace(terminal.writer());
    }

    @Override
    public String readPassword(String prompt) {
        String line = readLine(prompt, (char) 0);
        if (line == null) {
            throw new FatalCliException("Error reading password, terminal is closed");
        }
        return line;
    }

    @Override
    public String readLine(String prompt) {
        String attributedString = new AttributedString(prompt, DEFAULT.foreground(YELLOW)).toAnsi(terminal);
        return readLine(attributedString, null);
    }

    private String readLine(String prompt, Character mask) {
        try {
            String line = reader.readLine(prompt, null, mask, null);
            if (line == null) {
                throw new FatalCliException("Error reading password, terminal is closed");
            }
            return line;
        } catch (UserInterruptException ex) {
            return "";
        } catch (EndOfFileException ex) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        terminal.close();
    }

    public final class LineBuilder implements CliTerminal.LineBuilder {
        AttributedStringBuilder line;

        private LineBuilder() {
            line = new AttributedStringBuilder();
        }

        public LineBuilder text(String text) {
            line.append(text, DEFAULT);
            return this;
        }

        public LineBuilder em(String text) {
            line.append(text, DEFAULT.foreground(BRIGHT));
            return this;
        }


        public LineBuilder error(String text) {
            line.append(text, BOLD.foreground(RED));
            return this;
        }

        public LineBuilder param(String text) {
            line.append(text, DEFAULT.italic().foreground(YELLOW));
            return this;
        }

        public void ln() {
            terminal.writer().println(line.toAnsi(terminal));
        }

        public void end() {
            terminal.writer().print(line.toAnsi(terminal));
            terminal.writer().flush();
        }
    }

}
