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

import org.elasticsearch.common.compress.bzip2.CBZip2InputStream;
import org.xml.sax.InputSource;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.zip.GZIPInputStream;

/**
 * @author Delip Rao
 * @author Jason Smith
 */
public abstract class WikiXMLParser {

    private URL wikiXMLFile = null;
    protected WikiPage currentPage = null;

    public WikiXMLParser(URL fileName) {
        wikiXMLFile = fileName;
    }

    /**
     * Set a callback handler. The callback is executed every time a
     * page instance is detected in the stream. Custom handlers are
     * implementations of {@link PageCallbackHandler}
     *
     * @param handler
     * @throws Exception
     */
    public abstract void setPageCallback(PageCallbackHandler handler) throws Exception;

    /**
     * The main parse method.
     *
     * @throws Exception
     */
    public abstract void parse() throws Exception;

    /**
     * @return an iterator to the list of pages
     * @throws Exception
     */
    public abstract WikiPageIterator getIterator() throws Exception;

    /**
     * @return An InputSource created from wikiXMLFile
     * @throws Exception
     */
    protected InputSource getInputSource() throws Exception {
        BufferedReader br = null;

        if (wikiXMLFile.toExternalForm().endsWith(".gz")) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(wikiXMLFile.openStream()), "UTF-8"));
        } else if (wikiXMLFile.toExternalForm().endsWith(".bz2")) {
            InputStream fis = wikiXMLFile.openStream();
            byte[] ignoreBytes = new byte[2];
            fis.read(ignoreBytes); //"B", "Z" bytes from commandline tools
            br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis), "UTF-8"));
        } else {
            br = new BufferedReader(new InputStreamReader(wikiXMLFile.openStream(), "UTF-8"));
        }

        return new InputSource(br);
    }

    protected void notifyPage(WikiPage page) {
        currentPage = page;

    }
}
