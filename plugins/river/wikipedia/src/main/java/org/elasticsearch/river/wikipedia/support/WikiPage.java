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

import java.util.List;

/**
 * Data structures for a wikipedia page.
 *
 * @author Delip Rao
 */
public class WikiPage {

    private String title = null;
    private WikiTextParser wikiTextParser = null;
    private String id = null;

    /**
     * Set the page title. This is not intended for direct use.
     *
     * @param title
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Set the wiki text associated with this page.
     * This setter also introduces side effects. This is not intended for direct use.
     *
     * @param wtext wiki-formatted text
     */
    public void setWikiText(String wtext) {
        wikiTextParser = new WikiTextParser(wtext);
    }

    /**
     * @return a string containing the page title.
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param languageCode
     * @return a string containing the title translated
     *         in the given languageCode.
     */
    public String getTranslatedTitle(String languageCode) {
        return wikiTextParser.getTranslatedTitle(languageCode);
    }

    /**
     * @return true if this a disambiguation page.
     */
    public boolean isDisambiguationPage() {
        if (title.contains("(disambiguation)") ||
                wikiTextParser.isDisambiguationPage())
            return true;
        else return false;
    }

    /**
     * @return true for "special pages" -- like Category:, Wikipedia:, etc
     */
    public boolean isSpecialPage() {
        return title.contains(":");
    }

    /**
     * Use this method to get the wiki text associated with this page.
     * Useful for custom processing the wiki text.
     *
     * @return a string containing the wiki text.
     */
    public String getWikiText() {
        return wikiTextParser.getText();
    }

    /**
     * @return true if this is a redirection page
     */
    public boolean isRedirect() {
        return wikiTextParser.isRedirect();
    }

    /**
     * @return true if this is a stub page
     */
    public boolean isStub() {
        return wikiTextParser.isStub();
    }

    /**
     * @return the title of the page being redirected to.
     */
    public String getRedirectPage() {
        return wikiTextParser.getRedirectText();
    }

    /**
     * @return plain text stripped of all wiki formatting.
     */
    public String getText() {
        return wikiTextParser.getPlainText();
    }

    /**
     * @return a list of categories the page belongs to, null if this a redirection/disambiguation page
     */
    public List<String> getCategories() {
        return wikiTextParser.getCategories();
    }

    /**
     * @return a list of links contained in the page
     */
    public List<String> getLinks() {
        return wikiTextParser.getLinks();
    }

    public void setID(String id) {
        this.id = id;
    }

    public InfoBox getInfoBox() {
        return wikiTextParser.getInfoBox();
    }

    public String getID() {
        return id;
    }
}
