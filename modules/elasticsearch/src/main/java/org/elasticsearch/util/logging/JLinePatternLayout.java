/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.logging;

import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.helpers.FormattingInfo;
import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.util.jline.ANSI;

import java.lang.reflect.Field;

import static jline.ANSIBuffer.ANSICodes.*;
import static org.elasticsearch.util.jline.ANSI.Code.FG_BLUE;
import static org.elasticsearch.util.jline.ANSI.Code.FG_CYAN;
import static org.elasticsearch.util.jline.ANSI.Code.FG_GREEN;
import static org.elasticsearch.util.jline.ANSI.Code.FG_RED;
import static org.elasticsearch.util.jline.ANSI.Code.FG_YELLOW;
import static org.elasticsearch.util.jline.ANSI.Code.OFF;

/**
 * @author kimchy (Shay Banon)
 */
public class JLinePatternLayout extends PatternLayout {

    @Override protected PatternParser createPatternParser(String pattern) {
        try {
            return new JLinePatternParser(pattern);
        } catch (Throwable t) {
            return super.createPatternParser(pattern);
        }
    }

    private final static class JLinePatternParser extends PatternParser {

        private JLinePatternParser(String pattern) {
            super(pattern);
        }

        @Override protected void addConverter(PatternConverter pc) {
            try {
                if (ANSI.isEnabled()) {
                    if (pc.getClass().getName().endsWith("BasicPatternConverter")) {
                        Field typeField = pc.getClass().getDeclaredField("type");
                        typeField.setAccessible(true);
                        Integer type = (Integer) typeField.get(pc);
                        if (type == 2002) {
                            pc = new ColoredLevelPatternConverter(formattingInfo);
                        }
                    }
                }
            } catch (Throwable t) {
                // ignore
            }
            super.addConverter(pc);
        }

        private static class ColoredLevelPatternConverter extends PatternConverter {

            ColoredLevelPatternConverter(FormattingInfo formattingInfo) {
                super(formattingInfo);
            }

            public String convert(LoggingEvent event) {
                if (!ANSI.isEnabled()) {
                    return event.getLevel().toString();
                }
                if (event.getLevel() == Level.FATAL) {
                    return attrib(FG_RED) + event.getLevel().toString() + attrib(OFF);
                } else if (event.getLevel() == Level.ERROR) {
                    return attrib(FG_RED) + event.getLevel().toString() + attrib(OFF);
                } else if (event.getLevel() == Level.WARN) {
                    return attrib(FG_YELLOW) + event.getLevel().toString() + ' ' + attrib(OFF);
                } else if (event.getLevel() == Level.INFO) {
                    return attrib(FG_GREEN) + event.getLevel().toString() + ' ' + attrib(OFF);
                } else if (event.getLevel() == Level.DEBUG) {
                    return attrib(FG_CYAN) + event.getLevel().toString() + attrib(OFF);
                } else if (event.getLevel() == Level.TRACE) {
                    return attrib(FG_BLUE) + event.getLevel().toString() + attrib(OFF);
                }
                return event.getLevel().toString();
            }
        }
    }
}
