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

package org.elasticsearch.mapper.attachments;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.io.Streams.copy;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.mapper.attachments.AttachmentUnitTestCase.getIndicesModuleWithRegisteredAttachmentMapper;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;

/**
 * This class provides a simple main class which can be used to test what is extracted from a given binary file.
 * You can run it using
 *  -u file://URL/TO/YOUR/DOC
 *  --size set extracted size (default to mapper attachment size)
 *  BASE64 encoded binary
 *
 * Example:
 *  StandaloneRunner BASE64Text
 *  StandaloneRunner -u /tmp/mydoc.pdf
 *  StandaloneRunner -u /tmp/mydoc.pdf --size 1000000
 */
@SuppressForbidden(reason = "commandline tool")
public class StandaloneRunner extends CliTool {

    private static final CliToolConfig CONFIG = CliToolConfig.config("tika", StandaloneRunner.class)
                        .cmds(TikaRunner.CMD)
                .build();

    static {
        System.setProperty("es.path.home", "/tmp");
    }

    static class TikaRunner extends Command {
        private static final String NAME = "tika";
        private final String url;
        private final Integer size;
        private final String base64text;
        private final DocumentMapper docMapper;

        private static final CliToolConfig.Cmd CMD = cmd(NAME, TikaRunner.class)
                .options(option("u", "url").required(false).hasArg(false))
                .options(option("t", "size").required(false).hasArg(false))
                .build();

        protected TikaRunner(Terminal terminal, String url, Integer size, String base64text) throws IOException {
            super(terminal);
            this.size = size;
            this.url = url;
            this.base64text = base64text;
            DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(PathUtils.get("."), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser(); // use CWD b/c it won't be used

            String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/standalone/standalone-mapping.json");
            docMapper = mapperParser.parse("person", new CompressedXContent(mapping));
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            XContentBuilder builder = jsonBuilder().startObject().field("file").startObject();

            if (base64text != null) {
                // If base64 is provided
                builder.field("_content", base64text);
            } else {
                // A file is provided
                byte[] bytes = copyToBytes(PathUtils.get(url));
                builder.field("_content", bytes);
            }

            if (size >= 0) {
                builder.field("_indexed_chars", size);
            }

            BytesReference json = builder.endObject().endObject().bytes();

            ParseContext.Document doc = docMapper.parse("person", "person", "1", json).rootDoc();

            terminal.println("## Extracted text");
            terminal.println("--------------------- BEGIN -----------------------");
            terminal.println("%s", doc.get("file.content"));
            terminal.println("---------------------- END ------------------------");
            terminal.println("## Metadata");
            printMetadataContent(doc, AttachmentMapper.FieldNames.AUTHOR);
            printMetadataContent(doc, AttachmentMapper.FieldNames.CONTENT_LENGTH);
            printMetadataContent(doc, AttachmentMapper.FieldNames.CONTENT_TYPE);
            printMetadataContent(doc, AttachmentMapper.FieldNames.DATE);
            printMetadataContent(doc, AttachmentMapper.FieldNames.KEYWORDS);
            printMetadataContent(doc, AttachmentMapper.FieldNames.LANGUAGE);
            printMetadataContent(doc, AttachmentMapper.FieldNames.NAME);
            printMetadataContent(doc, AttachmentMapper.FieldNames.TITLE);

            return ExitStatus.OK;
        }

        private void printMetadataContent(ParseContext.Document doc, String field) {
            terminal.println("- %s: %s", field, doc.get(docMapper.mappers().getMapper("file." + field).fieldType().name()));
        }

        public static byte[] copyToBytes(Path path) throws IOException {
            try (InputStream is = Files.newInputStream(path)) {
                if (is == null) {
                    throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
                }
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    copy(is, out);
                    return out.bytes().toBytes();
                }
            }
        }

        public static Command parse(Terminal terminal, CommandLine cli) throws IOException {
            String url = cli.getOptionValue("u");
            String base64text = null;
            String sSize = cli.getOptionValue("size");
            Integer size = sSize != null ? Integer.parseInt(sSize) : -1;
            if (url == null && cli.getArgs().length == 0) {
                    return exitCmd(ExitStatus.USAGE, terminal, "url or BASE64 content should be provided (type -h for help)");
            }
            if (url == null) {
                if (cli.getArgs().length == 0) {
                    return exitCmd(ExitStatus.USAGE, terminal, "url or BASE64 content should be provided (type -h for help)");
                }
                base64text = cli.getArgs()[0];
            } else {
                if (cli.getArgs().length == 1) {
                    return exitCmd(ExitStatus.USAGE, terminal, "url or BASE64 content should be provided. Not both. (type -h for help)");
                }
            }
            return new TikaRunner(terminal, url, size, base64text);
        }
    }

    public StandaloneRunner() {
        super(CONFIG);
    }


    public static void main(String[] args) {
        StandaloneRunner pluginManager = new StandaloneRunner();
        pluginManager.execute(args);
    }

    @Override
    protected Command parse(String cmdName, CommandLine cli) throws Exception {
        switch (cmdName.toLowerCase(Locale.ROOT)) {
            case TikaRunner.NAME: return TikaRunner.parse(terminal, cli);
            default:
                    assert false : "can't get here as cmd name is validated before this method is called";
                    return exitCmd(ExitStatus.CODE_ERROR);
        }
    }
}
