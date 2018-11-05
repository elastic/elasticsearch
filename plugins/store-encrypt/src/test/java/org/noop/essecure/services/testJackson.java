package org.noop.essecure.services;

import java.io.IOException;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;


class testJackson {

	@Test
	void test() {

		String dek = "abcdefghijk";
    	String carJson = "{ \"dek\":\"" + dek + "\",\"ret_code\":0,\"ret_msg\":\"success\"}";
		EncryptionKey key = new EncryptionKey();
		try {
			EncryptionKey.parseFromDSMJSon(key, carJson);
			System.out.println(key.key);
			System.out.println(dek);
			assert(key.key.equals(dek));
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		/*
		String message = "{\"test\":\"test\"}";
		XContentBuilder b;
		try {
			b = XContentFactory.jsonBuilder().prettyPrint();
			try (XContentParser p = 
				XContentFactory.xContent(XContentType.JSON)
					.createParser(NamedXContentRegistry.EMPTY, null, message)) {

				XContentParser.Token token = null;
				while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
						System.out.println(token);
				}
				b.copyCurrentStructure(p);
			}
			System.out.println(b.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
		
	}

}
