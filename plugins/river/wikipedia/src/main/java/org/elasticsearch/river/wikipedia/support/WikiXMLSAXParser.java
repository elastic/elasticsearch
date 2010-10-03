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

package org.elasticsearch.river.wikipedia.support;

import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.net.URL;

/**
 * A SAX Parser for Wikipedia XML dumps.
 *
 * @author Jason Smith
 */
public class WikiXMLSAXParser extends WikiXMLParser {

    private XMLReader xmlReader;
    private PageCallbackHandler pageHandler = null;

    public WikiXMLSAXParser(URL fileName) {
        super(fileName);
        try {
            xmlReader = XMLReaderFactory.createXMLReader();
            pageHandler = new IteratorHandler(this);
        } catch (SAXException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Set a callback handler. The callback is executed every time a
     * page instance is detected in the stream. Custom handlers are
     * implementations of {@link PageCallbackHandler}
     *
     * @param handler
     * @throws Exception
     */
    public void setPageCallback(PageCallbackHandler handler) throws Exception {
        pageHandler = handler;
    }

    /**
     * The main parse method.
     *
     * @throws Exception
     */
    public void parse() throws Exception {
        xmlReader.setContentHandler(new SAXPageCallbackHandler(pageHandler));
        xmlReader.parse(getInputSource());
    }

    /**
     * This parser is event driven, so it
     * can't provide a page iterator.
     */
    @Override
    public WikiPageIterator getIterator() throws Exception {
        if (!(pageHandler instanceof IteratorHandler)) {
            throw new Exception("Custom page callback found. Will not iterate.");
        }
        throw new UnsupportedOperationException();
    }

    /**
     * A convenience method for the Wikipedia SAX interface
     *
     * @param dumpFile - path to the Wikipedia dump
     * @param handler  - callback handler used for parsing
     * @throws Exception
     */
    public static void parseWikipediaDump(URL dumpFile,
                                          PageCallbackHandler handler) throws Exception {
        WikiXMLParser wxsp = WikiXMLParserFactory.getSAXParser(dumpFile);
        wxsp.setPageCallback(handler);
	  wxsp.parse();
	}
	
}
