package org.elasticsearch.search.additional;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** 
 * Builder for additional parameters query section. 
 */
public class AdditionalParametersBuilder implements ToXContent {
  private Map<String, String> parameters;
  
  public AdditionalParametersBuilder() {
    parameters = new HashMap<String, String>();
  }
  
  public AdditionalParametersBuilder addParam(String name, String value) {
    parameters.put(name, value);
    return this;
  }
  
  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params)
      throws IOException {
    builder.startObject("additional");
    if (!parameters.isEmpty()) {
      for (Map.Entry<String, String> entry : parameters.entrySet()) {
        if (entry.getKey() != null && !entry.getKey().isEmpty() && entry.getValue() != null && !entry.getValue().isEmpty()) {
          builder.field(entry.getKey(), entry.getValue());
        }
      }
    }
    builder.endObject();
    return builder;
  }
}
