/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.elasticsearch.common.collect.ImmutableMap;

import java.util.Collection;

/**
*
*/
public class CliToolConfig {

    public static Builder config(String name, Class<? extends CliTool> toolType) {
        return new Builder(name, toolType);
    }

    private final Class<? extends CliTool> toolType;
    private final String name;
    private final ImmutableMap<String, Cmd> cmds;

    private static final HelpPrinter helpPrinter = new HelpPrinter();

    private CliToolConfig(String name, Class<? extends CliTool> toolType, Cmd[] cmds) {
        this.name = name;
        this.toolType = toolType;
        ImmutableMap.Builder<String, Cmd> cmdsBuilder = ImmutableMap.builder();
        for (int i = 0; i < cmds.length; i++) {
            cmdsBuilder.put(cmds[i].name, cmds[i]);
        }
        this.cmds = cmdsBuilder.build();
    }

    public boolean isSingle() {
        return cmds.size() == 1;
    }

    public Cmd single() {
        assert isSingle() : "Requesting single command on a multi-command tool";
        return cmds.values().iterator().next();
    }

    public Class<? extends CliTool> toolType() {
        return toolType;
    }

    public String name() {
        return name;
    }

    public Collection<Cmd> cmds() {
        return cmds.values();
    }

    public Cmd cmd(String name) {
        return cmds.get(name);
    }

    public void printUsage(Terminal terminal) {
        helpPrinter.print(this, terminal);
    }

    public static class Builder {

        public static Cmd.Builder cmd(String name, Class<? extends CliTool.Command> cmdType) {
            return new Cmd.Builder(name, cmdType);
        }

        public static OptionBuilder option(String shortName, String longName) {
            return new OptionBuilder(shortName, longName);
        }

        private final Class<? extends CliTool> toolType;
        private final String name;
        private Cmd[] cmds;

        private Builder(String name, Class<? extends CliTool> toolType) {
            this.name = name;
            this.toolType = toolType;
        }

        public Builder cmds(Cmd.Builder... cmds) {
            this.cmds = new Cmd[cmds.length];
            for (int i = 0; i < cmds.length; i++) {
                this.cmds[i] = cmds[i].build();
            }
            return this;
        }

        public Builder cmds(Cmd... cmds) {
            this.cmds = cmds;
            return this;
        }

        public CliToolConfig build() {
            return new CliToolConfig(name, toolType, cmds);
        }
    }

    public static class Cmd {

        private final String name;
        private final Class<? extends CliTool.Command> cmdType;
        private final Options options;

        private Cmd(String name, Class<? extends CliTool.Command> cmdType, Options options) {
            this.name = name;
            this.cmdType = cmdType;
            this.options = options;
            this.options.addOption(new OptionBuilder("h", "help").required(false).build());
        }

        public Class<? extends CliTool.Command> cmdType() {
            return cmdType;
        }

        public String name() {
            return name;
        }

        public Options options() {
            return options;
        }

        public void printUsage(Terminal terminal) {
            helpPrinter.print(this, terminal);
        }

        public static class Builder {

            private final String name;
            private final Class<? extends CliTool.Command> cmdType;
            private Options options = new Options();

            private Builder(String name, Class<? extends CliTool.Command> cmdType) {
                this.name = name;
                this.cmdType = cmdType;
            }

            public Builder options(OptionBuilder... optionBuilder) {
                for (int i = 0; i < optionBuilder.length; i++) {
                    options.addOption(optionBuilder[i].build());
                }
                return this;
            }

            public Cmd build() {
                return new Cmd(name, cmdType, options);
            }
        }
    }

    public static class OptionBuilder {

        private final Option option;

        private OptionBuilder(String shortName, String longName) {
            option = new Option(shortName, "");
            option.setLongOpt(longName);
            option.setArgName(longName);
        }

        public OptionBuilder required(boolean required) {
            option.setRequired(required);
            return this;
        }

        public OptionBuilder hasArg(boolean optional) {
            option.setOptionalArg(optional);
            option.setArgs(1);
            return this;
        }

        public Option build() {
            return option;
        }
    }
}
