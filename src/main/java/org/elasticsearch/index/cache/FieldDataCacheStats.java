package org.elasticsearch.index.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.cache.field.data.support.AbstractConcurrentMapFieldDataCache;
import org.elasticsearch.index.service.IndexService;

public class FieldDataCacheStats implements ToXContent {
	private Map<String, HashMap<String, Object>> fieldCaches;

	public FieldDataCacheStats() {
		fieldCaches = new HashMap<String, HashMap<String, Object>>();
	}

	public void add(IndexService indexService) {
		HashMap<String, Object> tmp = new HashMap<String, Object>();

		AbstractConcurrentMapFieldDataCache fieldCache = (AbstractConcurrentMapFieldDataCache) indexService.cache().fieldData();

		//Get the cache size for all fields of the given index
		for (String elem : fieldCache.getFieldNames()) {
			long cacheSize = fieldCache.sizeInBytes(elem);
			tmp.put(elem, cacheSize);
		}

		//Put the whole fieldcache map into the global map
		fieldCaches.put(indexService.index().getName(), tmp);
	}

	@Override
	public XContentBuilder toXContent(XContentBuilder builder, Params params)
			throws IOException {
		builder.startObject(Fields.FIELDDATACACHES);

		Iterator<String> iterator = this.fieldCaches.keySet().iterator();
		while(iterator.hasNext()) {
			String key = iterator.next();
			HashMap<String, Object> tmp = this.fieldCaches.get(key);
			
			//Append the map with all the fieldcaches and their size
			builder.field(key, tmp);
		}

		builder.endObject();
		return builder;
	}
	
	static final class Fields {
		static final XContentBuilderString FIELDDATACACHES = new XContentBuilderString("fieldDataCaches");
	}
}
