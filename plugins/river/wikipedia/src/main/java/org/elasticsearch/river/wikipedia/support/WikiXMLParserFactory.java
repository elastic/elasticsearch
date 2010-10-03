package org.elasticsearch.river.wikipedia.support;

import java.net.URL;

/**
 * @author Delip Rao
 */
public class WikiXMLParserFactory {

    public static WikiXMLParser getSAXParser(URL fileName) {
        return new WikiXMLSAXParser(fileName);
    }

}
