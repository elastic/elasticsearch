/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Parses command line strings into a list of arguments following shell-specific
 * quoting and escaping rules. Supports bash (POSIX), Windows cmd
 * (CommandLineToArgvW), and PowerShell syntax.
 */
final class CommandLineParser {

    /**
     * The shell syntax to use when parsing command lines.
     */
    enum ShellType {
        /**
         * POSIX/bash shell rules. Single quotes preserve everything literally,
         * double quotes allow backslash escaping of {@code "}, {@code $},
         * {@code `}, {@code \}, and {@code !}. Unquoted backslashes escape any
         * following character.
         */
        BASH,

        /**
         * Windows CommandLineToArgvW (post-2008 MSVC) rules. Uses double quotes
         * and backslash-before-quote escaping. Inside a quoted region,
         * {@code ""} produces a literal {@code "} and continues the quoted
         * region.
         */
        CMD,

        /**
         * PowerShell rules. Backtick ({@code `}) is the escape character.
         * Single quotes preserve everything literally except {@code ''}
         * which produces a single {@code '}. Double quotes support backtick
         * escaping and {@code ""} for a literal {@code "}.
         */
        POWERSHELL;

        static ShellType fromString(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "bash", "sh" -> BASH;
                case "cmd" -> CMD;
                case "powershell" -> POWERSHELL;
                default -> throw new IllegalArgumentException(
                    "unsupported shell type [" + value + "]; supported types are [bash], [cmd], and [powershell]"
                );
            };
        }
    }

    private CommandLineParser() {}

    /**
     * Parses a command line string into a list of arguments using the specified
     * shell syntax.
     *
     * @param commandLine the command line string to parse
     * @param shell       the shell syntax to use
     * @return a list of parsed arguments
     */
    static List<String> parse(String commandLine, ShellType shell) {
        return switch (shell) {
            case BASH -> parseBash(commandLine);
            case CMD -> parseCmd(commandLine);
            case POWERSHELL -> parsePowerShell(commandLine);
        };
    }

    /**
     * Parses a command line using POSIX/bash quoting rules.
     * <p>
     * Rules:
     * <ul>
     *   <li>Unquoted whitespace separates arguments</li>
     *   <li>Backslash outside quotes escapes any following character</li>
     *   <li>Single quotes: everything is literal until the closing {@code '} (no escape mechanism)</li>
     *   <li>Double quotes: backslash only escapes {@code "}, {@code $}, {@code `}, {@code \}, and {@code !};
     *       all other backslash sequences are preserved literally</li>
     *   <li>Empty strings ({@code ""} or {@code ''}) produce an empty argument</li>
     * </ul>
     */
    private static List<String> parseBash(String commandLine) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inArg = false;
        int i = 0;
        int len = commandLine.length();

        while (i < len) {
            char c = commandLine.charAt(i);

            if (c == '\\') {
                if (i + 1 < len) {
                    // unquoted backslash: escape next character
                    inArg = true;
                    i++;
                    current.append(commandLine.charAt(i));
                }
                // trailing backslash with nothing after it is dropped
            } else if (c == '\'') {
                // single-quoted string: everything literal until closing '
                inArg = true;
                i++;
                while (i < len && commandLine.charAt(i) != '\'') {
                    current.append(commandLine.charAt(i));
                    i++;
                }
                // skip closing quote (if present)
            } else if (c == '"') {
                // double-quoted string
                inArg = true;
                i++;
                while (i < len && commandLine.charAt(i) != '"') {
                    if (commandLine.charAt(i) == '\\' && i + 1 < len) {
                        char next = commandLine.charAt(i + 1);
                        if (next == '"' || next == '$' || next == '`' || next == '\\' || next == '!') {
                            current.append(next);
                            i += 2;
                            continue;
                        }
                    }
                    current.append(commandLine.charAt(i));
                    i++;
                }
                // skip closing quote (if present)
            } else if (c == ' ' || c == '\t') {
                if (inArg) {
                    args.add(current.toString());
                    current.setLength(0);
                    inArg = false;
                }
                // skip whitespace
            } else {
                inArg = true;
                current.append(c);
            }
            i++;
        }

        if (inArg) {
            args.add(current.toString());
        }

        return args;
    }

    /**
     * Parses a command line using Windows CommandLineToArgvW rules (post-2008 MSVC).
     * <p>
     * Rules:
     * <ul>
     *   <li>{@code 2n} backslashes before {@code "} produce {@code n} backslashes; the {@code "} toggles quoting</li>
     *   <li>{@code 2n+1} backslashes before {@code "} produce {@code n} backslashes plus a literal {@code "}</li>
     *   <li>Backslashes not immediately before {@code "} are literal</li>
     *   <li>Inside a quoted region, {@code ""} produces one literal {@code "} and continues quoted mode</li>
     *   <li>Whitespace outside quotes separates arguments</li>
     * </ul>
     */
    private static List<String> parseCmd(String commandLine) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inArg = false;
        boolean inQuotes = false;
        int i = 0;
        int len = commandLine.length();

        while (i < len) {
            char c = commandLine.charAt(i);

            if (c == '\\') {
                // count consecutive backslashes
                int numBackslashes = 0;
                while (i < len && commandLine.charAt(i) == '\\') {
                    numBackslashes++;
                    i++;
                }

                if (i < len && commandLine.charAt(i) == '"') {
                    // backslashes followed by a double quote
                    inArg = true;
                    // add floor(n/2) literal backslashes
                    for (int j = 0; j < numBackslashes / 2; j++) {
                        current.append('\\');
                    }
                    if (numBackslashes % 2 == 1) {
                        // odd number: last backslash escapes the quote -> literal "
                        current.append('"');
                        i++;
                    }
                    // even number: the " is a quoting character, handled in next iteration
                } else {
                    // backslashes not before a quote: all literal
                    inArg = true;
                    for (int j = 0; j < numBackslashes; j++) {
                        current.append('\\');
                    }
                }
            } else if (c == '"') {
                inArg = true;
                if (inQuotes) {
                    // inside quotes: check for ""
                    if (i + 1 < len && commandLine.charAt(i + 1) == '"') {
                        // "" inside quotes -> literal "
                        current.append('"');
                        i += 2;
                    } else {
                        // single " -> end quoted block
                        inQuotes = false;
                        i++;
                    }
                } else {
                    // outside quotes: begin quoted block
                    inQuotes = true;
                    i++;
                }
            } else if ((c == ' ' || c == '\t') && !inQuotes) {
                if (inArg) {
                    args.add(current.toString());
                    current.setLength(0);
                    inArg = false;
                }
                i++;
            } else {
                inArg = true;
                current.append(c);
                i++;
            }
        }

        if (inArg) {
            args.add(current.toString());
        }

        return args;
    }

    /**
     * Parses a command line using PowerShell quoting rules.
     * <p>
     * Rules:
     * <ul>
     *   <li>Backtick ({@code `}) is the escape character outside single quotes</li>
     *   <li>Single quotes: everything literal until {@code '}; {@code ''} produces a literal {@code '}</li>
     *   <li>Double quotes: backtick escaping is active; {@code ""} produces a literal {@code "};
     *       known backtick sequences like {@code `n}, {@code `t} are expanded</li>
     *   <li>Whitespace outside quotes separates arguments</li>
     * </ul>
     */
    private static List<String> parsePowerShell(String commandLine) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inArg = false;
        int i = 0;
        int len = commandLine.length();

        while (i < len) {
            char c = commandLine.charAt(i);

            if (c == '`' && i + 1 < len) {
                // backtick escape (outside any quote)
                inArg = true;
                i++;
                current.append(expandPowerShellEscape(commandLine.charAt(i)));
            } else if (c == '\'') {
                // single-quoted string: everything literal, '' -> '
                inArg = true;
                i++;
                while (i < len) {
                    if (commandLine.charAt(i) == '\'') {
                        if (i + 1 < len && commandLine.charAt(i + 1) == '\'') {
                            current.append('\'');
                            i += 2;
                        } else {
                            break; // closing quote
                        }
                    } else {
                        current.append(commandLine.charAt(i));
                        i++;
                    }
                }
                // skip closing quote (if present)
            } else if (c == '"') {
                // double-quoted string: backtick escaping, "" -> "
                inArg = true;
                i++;
                while (i < len) {
                    char dc = commandLine.charAt(i);
                    if (dc == '"') {
                        if (i + 1 < len && commandLine.charAt(i + 1) == '"') {
                            current.append('"');
                            i += 2;
                        } else {
                            break; // closing quote
                        }
                    } else if (dc == '`' && i + 1 < len) {
                        i++;
                        current.append(expandPowerShellEscape(commandLine.charAt(i)));
                        i++;
                    } else {
                        current.append(dc);
                        i++;
                    }
                }
                // skip closing quote (if present)
            } else if (c == ' ' || c == '\t') {
                if (inArg) {
                    args.add(current.toString());
                    current.setLength(0);
                    inArg = false;
                }
                // skip whitespace
            } else {
                inArg = true;
                current.append(c);
            }
            i++;
        }

        if (inArg) {
            args.add(current.toString());
        }

        return args;
    }

    /**
     * Expands PowerShell backtick escape sequences to their character equivalents.
     */
    private static char expandPowerShellEscape(char c) {
        return switch (c) {
            case 'n' -> '\n';
            case 't' -> '\t';
            case 'r' -> '\r';
            case 'a' -> '\u0007'; // bell
            case 'b' -> '\b';
            case 'f' -> '\f';
            case '0' -> '\0';
            default -> c; // backtick before any other character just produces that character
        };
    }
}
