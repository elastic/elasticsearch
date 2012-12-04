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

package org.elasticsearch.index.mapper.object;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * A DynamicTemaplate is used to generate mappings for fields for which there are not existing 
 * entries in the type mapping associated with a document. DynamicTemplate instances are owned by
 * instance of the {@link RootObjectMapper}. 
 * <p>
 * A DynamicTemplate instance is initialized by parsing an entry from the <code>dynamic_templates</code>
 * array within a type mapping. A typical usage example is shown below:    
 * <pre>
 *  "template_1" : {
 *    "match": "*__*",
 *    "capture":"(([a-zA-Z0-9]+?)(?:_[a-zA-Z0-9]+)*)__[a-zA-Z0-9]+", 
 *    "options" :{
 *      "n":{"index_name":"{1}"},
 *      "a":{"include_in_all":false},
 *      "1":{"analyzer":"myAnalyzer"},
 *      "f":{"index":"not_analyzed"},
 *      "f":{"index":"no"},
 *      "d":{"dynamic":false},
 *      "e":{"enabled":false},
 *      "s":{"store":true} 
 *     },
 *     "mapping": {
 *       "index_name":"{2}"
 *     }
 *   }
 * </pre>  
 * Parameters not shown in the above example: <code>path_match</code>, <code>unmatch</code>,
 * <code>path_unmatch</code>, <code>match_mapping_type</code> and <code>match_pattern</code>.
 * <p>
 *  The name, path and field type detected dynamically by ElasticSearch (dynamicType) must satisfy
 *  the template's matching criteria in order for ElasticSearch to use the template to generate a
 *  mapping for the field:
 * <p>
 *  <ul>
 *  <li>The name of the field must match the <code>match</code> pattern if it is present
 *  <li>The name of the field must not match the <code>unmatch</code> pattern if it is present
 *  <li>The full path of the field must match the <code>path_match</code> pattern if it is present
 *  <li>The full path of the field must not match the <code>path_match</code> pattern if it is present
 *  <li>The type of the field detected by ElasticSearch must match the <code>match_mapping_type</code> pattern if it is present
 *  <li><code>match_pattern</code> sets the matching method used: <code>simple</code> (where * is a wildcard) or <code>regex</code> (regular expression) 
 *  <li>At least one of <code>match</code> or <code>path_match</code> must be present.
 *  </ul>
 *  <p>
 *  Once the template matching criteria have been met the <code>mapping</code> and <code>options</code> 
 *  elements of the template are used to generate a concrete mapping as follows:
 * <p>
 *  <ul>
 *  <li>The dynamic template builds a parameter map containing mappings for the default parameters
 *      {name}, {dynamicType}, {dynamic_type), {path}, {rawPath} and {raw_path}. The {path} 
 *      parameter is the full path to the field with all option information stripped out (see below)
 *      while the raw paths contain the option information.
 *  <li>If a <code>capture</code> regular expression pattern is provided it is used to extract
 *      substrings from the <code>name</code> of the field. The <code>capture</code> pattern must
 *      define capture groups, as per regular expression syntax, to capture parts of the field name
 *      as required. Key/value pairs associated with all captured groups are added to the parameter
 *      map using keys of the for "{n}" where n is the group number. For example, the  capture 
 *      pattern <code>(([a-zA-Z0-9]+?)(?:_[a-zA-Z0-9]+)*)__[a-zA-Z0-9]+</code> used with 
 *      a field named <code>"first_name__s"</code> will result in the capture of parameters 
 *      <code>{1}=>first_name</code> and <code>{2}=>first</code>. 
 *  <li>If the field <code>name</code> contains two sequential underscore characters, ie "__", then 
 *      all characters after them are interpreted as a list of option keys. These option keys are 
 *      used to look up entries from the options map defined by the <code>options</code> element
 *      in the dynamic template definition. 
 *  <li>The dynamic template merges all options specified by the option keys into the default 
 *      mapping defined by the <code>mapping</code> element in the dynamic template definition.
 *      The dynamic template performs parameter substitution on the default mapping and all
 *      options while performing the merge.
 *  <li>The <code>mapping</code> element must be present, even if it is empty.
 *  <li>The options list is optional.
 * </ul>
 */


public class DynamicTemplate {

    public static enum MatchType {
        SIMPLE,
        REGEX;

        public static MatchType fromString(String value) {
            if ("simple".equals(value)) {
                return SIMPLE;
            } else if ("regex".equals(value)) {
                return REGEX;
            }
            
            throw new ElasticSearchIllegalArgumentException("No matching pattern matched on [" + value + "]");
        }
    }

    //-------------------------------------------------------------------------------------------

    public static DynamicTemplate parse(String name, Map<String, Object> conf) throws MapperParsingException {
        String match = null;
        String pathMatch = null;
        String unmatch = null;
        String pathUnmatch = null;
        Map<String, Object> mapping = null;
        String matchMappingType = null;
        String matchPattern = "simple";
        Map<String, Map<String, Object>> options = null;
        String captureMask = null;

        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
            if ("match".equals(propName)) {
                match = entry.getValue().toString();
            } else if ("path_match".equals(propName)) {
                pathMatch = entry.getValue().toString();
            } else if ("unmatch".equals(propName)) {
                unmatch = entry.getValue().toString();
            } else if ("path_unmatch".equals(propName)) {
                pathUnmatch = entry.getValue().toString();
            } else if ("match_mapping_type".equals(propName)) {
                matchMappingType = entry.getValue().toString();
            } else if ("match_pattern".equals(propName)) {
                matchPattern = entry.getValue().toString();
            } else if ("mapping".equals(propName)) {
                mapping = (Map<String, Object>) entry.getValue();
            } else if ("options".equals(propName)) {
                options = (Map<String, Map<String, Object>>) entry.getValue();
            } else if ("capture".equals(propName)) {
                captureMask = entry.getValue().toString();
            }
        }

        if (match == null && pathMatch == null) {
            throw new MapperParsingException("template must have match or path_match set");
        }
        if (mapping == null) {
            throw new MapperParsingException("template must have mapping set");
        }
        return new DynamicTemplate(name, conf, pathMatch, pathUnmatch, match, unmatch, matchMappingType, MatchType.fromString(matchPattern), mapping, options, captureMask);
    }

    //-------------------------------------------------------------------------------------------

    
    private final String name;

    private final Map<String, Object> conf;

    private final String pathMatch;

    private final String pathUnmatch;

    private final String match;

    private final String unmatch;

    private final MatchType matchType;

    private final String matchMappingType;

    private final Map<String, Object> mapping;
    private final Map<String, Map<String, Object>> options;
    private Pattern capturePattern;
    
    //-------------------------------------------------------------------------------------------

    public DynamicTemplate(String name, Map<String, Object> conf, String pathMatch, String pathUnmatch,
                           String match, String unmatch, String matchMappingType, MatchType matchType,
                           Map<String, Object> mapping, Map<String, Map<String, Object>> options, String captureMask) {
        this.name = name;
        this.conf = new TreeMap<String, Object>(conf);
        this.pathMatch = pathMatch;
        this.pathUnmatch = pathUnmatch;
        this.match = match;
        this.unmatch = unmatch;
        this.matchType = matchType;
        this.matchMappingType = matchMappingType;
        this.mapping = mapping;
        this.options = (options != null) ? options : new HashMap<String, Map<String, Object>>();
        this.capturePattern = (captureMask != null) ? Pattern.compile(captureMask) : null;
    }

    //-------------------------------------------------------------------------------------------

    public String name() {
        return this.name;
    }

    //-------------------------------------------------------------------------------------------

    public Map<String, Object> conf() {
        return this.conf;
    }

    //-------------------------------------------------------------------------------------------

    public boolean match(ContentPath path, String name, String dynamicType) {
        if (pathMatch != null && !patternMatch(pathMatch, path.fullPathAsText(name))) {
            return false;
        }
        if (match != null && !patternMatch(match, name)) {
            return false;
        }
        if (pathUnmatch != null && patternMatch(pathUnmatch, path.fullPathAsText(name))) {
            return false;
        }
        if (unmatch != null && patternMatch(unmatch, name)) {
            return false;
        }
        if (matchMappingType != null) {
            if (dynamicType == null) {
                return false;
            }
            if (!patternMatch(matchMappingType, dynamicType)) {
                return false;
            }
        }

        return true;
    }

    //-------------------------------------------------------------------------------------------
    
    public boolean hasType(String name) {
        // If there are no options we just check the for an explicit type in the mapping
        String opts = getOpts(name);
        if (opts == null)
        {
          return mapping.containsKey("type");
        }

        // A template with options will explicitely set the type or throw an exception if the
        // generated mapping does not have a type. 
        return(true);
    }

    //-------------------------------------------------------------------------------------------

    public String mappingType(String name, String dynamicType) {
        // If there are no options then just use the type from the mapping or the dynamic type
        String opts = getOpts(name);
        if (opts == null)
        {
          return mapping.containsKey("type") ? mapping.get("type").toString() : dynamicType;
        }
          
        // We must calculate the mapping because the type may be determined by one of the 
        // options, there are no shortcuts. The mapping is guaranteed to have a type because
        // dynamicType is applied if no explicit type is found after merging options into the
        // default mapping.
        Map<String, Object> map = mappingForName(null, name, dynamicType);
        return((String)map.get("type"));
    }

    //-------------------------------------------------------------------------------------------

    private boolean patternMatch(String pattern, String str) {
        if (matchType == MatchType.SIMPLE) {
            return Regex.simpleMatch(pattern, str);
        }
        
        return str.matches(pattern);
    }

    //-------------------------------------------------------------------------------------------

    public Map<String, Object> mappingForName(ContentPath path, String name, String dynamicType) {
       
      // Get params and process the mapping
      Map<String, String> params = parseParams(path, name, dynamicType);
      Map<String, Object> processedMap = processMap(mapping, params);
              
      // Apply options
      String opts = getOpts(name);
      if (opts != null && opts.length() > 1)
      {
        for (int n = 0 ; n < opts.length() ; n++)
        {
          // Get the next option and do parameter substitution before adding to processedMap
          Map<String, Object> optNode = options.get(opts.substring(n, n + 1));
          if (optNode != null)
          {
            Map<String, Object> values = processMap(optNode, params);
            processedMap.putAll(values);
          }
        }
      }

      // Make sure that the type is explitely set
      if (!processedMap.containsKey("type"))
      {
        processedMap.put("type", dynamicType);
      }
      
      return processedMap;
    }    

    //-------------------------------------------------------------------------------------------
    /**
     * Parses the path, name and dynamic type to build map of parameters for substitution into
     * mapping and options.
     * 
     * @param path Path to field being mapped
     * @param name Name of field being mapped
     * @param dynamicType Type detected by JSON parser
     * @return Map of parameters, all keys contain opening and closing braces 
     */
    
    private Map<String, String> parseParams(ContentPath path, String name, String dynamicType)
    {
      HashMap<String, String> params = new HashMap<String, String>();

      // Add standard parameters
      params.put("{name}", name);
      params.put("{dynamic_type}", dynamicType);
      params.put("{dynamicType}", dynamicType);

      // Claculate and add path. Options are stripped out.
      StringBuilder builder = new StringBuilder();
      StringBuilder rawBuilder = new StringBuilder();
      if (path != null)
      {
        Iterator<String> iter = path.iterator();
        for (int n = 0 ; iter.hasNext() ; n++)
        {
          String node = iter.next();
          rawBuilder.append((n > 0) ? "." : "" ).append(node);
          int pos = node.lastIndexOf("__");
          builder.append((n > 0) ? "." : "" ).append((pos <= 0) ? node : node.substring(0, pos));
        }
      }
      params.put("{path}", builder.toString());
      params.put("{raw_path}", rawBuilder.toString());
      params.put("{rawPath}", rawBuilder.toString());
      
      // Parse the name using capture regular expression. Values for capture groups are added
      // to parameter map.
      if (capturePattern != null)
      {
        Matcher m = capturePattern.matcher(name);
        if (!m.matches())
        {
          throw new ElasticSearchIllegalArgumentException("Name [" + name + "] does not match capture pattern [" + capturePattern.pattern() + "]");
        }

        for (int n = 1 ; n <= m.groupCount() ; n++)
        {
          params.put("{"+ n + "}", m.group(n));
        }
      }
      
      return(params);
    }    
    
    //-------------------------------------------------------------------------------------------
    /**
     * Parses the name and extract the options suffix if present
     * @param name
     * @return 
     */
    
    private String getOpts(String name)
    {
      // Opts will only exist if options have been provided 
      int pos = name.lastIndexOf("__");
      if (pos > 0)
      {
        return(name.substring(pos + 2));
      }
      
      return null;
    }
    
    //-------------------------------------------------------------------------------------------

    private Map<String, Object> processMap(Map<String, Object> map, Map<String, String> params ) {
        Map<String, Object> processedMap = Maps.newHashMap();
        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            String key = doReplace(entry.getKey(), params);
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = processMap((Map<String, Object>) value, params);
            } else if (value instanceof List) {
                value = processList((List) value, params);
            } else if (value instanceof String) {
                value = doReplace(value.toString(), params);
            }
            processedMap.put(key, value);
        }
        return processedMap;
    }

    //-------------------------------------------------------------------------------------------

    private List processList(List list, Map<String, String> params) {
        List processedList = new ArrayList();
        for (Object value : list) {
            if (value instanceof Map) {
                value = processMap((Map<String, Object>) value, params);
            } else if (value instanceof List) {
                value = processList((List) value, params);
            } else if (value instanceof String) {
                value = doReplace(value.toString(), params);
            }
            processedList.add(value);
        }
        return processedList;
    }

    //-------------------------------------------------------------------------------------------

    private String doReplace(String src, Map<String, String> params )
    {
      // Iterate the params in the source because there will generally be fewer of these than
      // values in param map, maybe none at all.
      String res = src;
      int start;
      while ( (start = res.indexOf("{")) >= 0)
      {
        int end = res.indexOf("}", start + 1);
        if (end < 0)
        {
          throw new ElasticSearchIllegalArgumentException("Param substitution failure, malformed source [" + src + "]");
        }
        
        String key = res.substring(start, end + 1);
        String val = params.get(key);
        if (val == null)
        {
          throw new ElasticSearchIllegalArgumentException("Param substitution failure, invalid param [" + key + "]");
        }
        
        res = res.replace(key, val);
      }      
      
      return(res);
    }
    
    //-------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DynamicTemplate that = (DynamicTemplate) o;

        // check if same matching, if so, replace the mapping
        if (match != null ? !match.equals(that.match) : that.match != null) return false;
        if (matchMappingType != null ? !matchMappingType.equals(that.matchMappingType) : that.matchMappingType != null)
            return false;
        if (matchType != that.matchType) return false;
        if (unmatch != null ? !unmatch.equals(that.unmatch) : that.unmatch != null) return false;

        return true;
    }

    //-------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        // check if same matching, if so, replace the mapping
        int result = match != null ? match.hashCode() : 0;
        result = 31 * result + (unmatch != null ? unmatch.hashCode() : 0);
        result = 31 * result + (matchType != null ? matchType.hashCode() : 0);
        result = 31 * result + (matchMappingType != null ? matchMappingType.hashCode() : 0);
        return result;
    }
}
