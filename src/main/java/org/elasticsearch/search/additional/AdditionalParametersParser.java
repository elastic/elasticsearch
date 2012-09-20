package org.elasticsearch.search.additional;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Parser for additional parameters section.
 */
public class AdditionalParametersParser implements SearchParseElement {
  @Override
  public void parse(XContentParser parser, SearchContext context)
      throws Exception {
    XContentParser.Token token;
    String name = null, value = null;
    
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      if (token == XContentParser.Token.FIELD_NAME) {
        name = parser.text();
      } else if (token == XContentParser.Token.VALUE_STRING) {
        value = parser.text();
      } 
      if (name != null && value != null) {
        context.addAdditionalParameter(name, value);
        name = null;
        value = null;
      }
    }
  }
}
