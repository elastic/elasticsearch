/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.plugin.mapper.attachments.tika;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.SAXException;


/**
 * Extends the Tika class, so as to provide a way for setting the maximumStringLength on a per parse document basis.
 */
public class TikaExtended extends Tika {

	public String parseToString(InputStream stream, Metadata metadata, int maxExtractedStringLength) throws IOException, TikaException {
		
		// setup
		WriteOutContentHandler writeHandler = new WriteOutContentHandler(maxExtractedStringLength);
		BodyContentHandler contentHandler = new BodyContentHandler(writeHandler);
		Parser parser = getParser();
		ParseContext context = new ParseContext();
		context.set(Parser.class, parser);

		try {
			parser.parse(stream, contentHandler, metadata, context);
		} catch (SAXException e) {
			if (!writeHandler.isWriteLimitReached(e)) {
				throw new TikaException("Unexpected SAX processing failure", e);
			}
		} finally {
			stream.close();
		}

		return writeHandler.toString();
	}
}
