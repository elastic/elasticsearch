---
navigation_title: "IP prefix"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-ipprefix-aggregation.html
---

# IP prefix aggregation [search-aggregations-bucket-ipprefix-aggregation]


A bucket aggregation that groups documents based on the network or sub-network of an IP address. An IP address consists of two groups of bits: the most significant bits which represent the network prefix, and the least significant bits which represent the host.

## Example [ipprefix-agg-ex]

For example, consider the following index:

```console
PUT network-traffic
{
    "mappings": {
        "properties": {
            "ipv4": { "type": "ip" },
            "ipv6": { "type": "ip" }
        }
    }
}

POST /network-traffic/_bulk?refresh
{"index":{"_id":0}}
{"ipv4":"192.168.1.10","ipv6":"2001:db8:a4f8:112a:6001:0:12:7f10"}
{"index":{"_id":1}}
{"ipv4":"192.168.1.12","ipv6":"2001:db8:a4f8:112a:6001:0:12:7f12"}
{"index":{"_id":2}}
{ "ipv4":"192.168.1.33","ipv6":"2001:db8:a4f8:112a:6001:0:12:7f33"}
{"index":{"_id":3}}
{"ipv4":"192.168.1.10","ipv6":"2001:db8:a4f8:112a:6001:0:12:7f10"}
{"index":{"_id":4}}
{"ipv4":"192.168.2.41","ipv6":"2001:db8:a4f8:112c:6001:0:12:7f41"}
{"index":{"_id":5}}
{"ipv4":"192.168.2.10","ipv6":"2001:db8:a4f8:112c:6001:0:12:7f10"}
{"index":{"_id":6}}
{"ipv4":"192.168.2.23","ipv6":"2001:db8:a4f8:112c:6001:0:12:7f23"}
{"index":{"_id":7}}
{"ipv4":"192.168.3.201","ipv6":"2001:db8:a4f8:114f:6001:0:12:7201"}
{"index":{"_id":8}}
{"ipv4":"192.168.3.107","ipv6":"2001:db8:a4f8:114f:6001:0:12:7307"}
```

The following aggregation groups documents into buckets. Each bucket identifies a different sub-network. The sub-network is calculated by applying a netmask with prefix length of `24` to each IP address in the `ipv4` field:

$$$ip-prefix-ipv4-example$$$

```console
GET /network-traffic/_search
{
  "size": 0,
  "aggs": {
    "ipv4-subnets": {
      "ip_prefix": {
        "field": "ipv4",
        "prefix_length": 24
      }
    }
  }
}
```

Response:

```console-result
{
  ...

  "aggregations": {
    "ipv4-subnets": {
      "buckets": [
        {
          "key": "192.168.1.0",
          "is_ipv6": false,
          "doc_count": 4,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        },
        {
          "key": "192.168.2.0",
          "is_ipv6": false,
          "doc_count": 3,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        },
        {
           "key": "192.168.3.0",
           "is_ipv6": false,
           "doc_count": 2,
           "prefix_length": 24,
           "netmask": "255.255.255.0"
        }
      ]
    }
  }
}
```

To aggregate IPv6 addresses, set `is_ipv6` to `true`.

$$$ip-prefix-ipv6-example$$$

```console
GET /network-traffic/_search
{
  "size": 0,
  "aggs": {
    "ipv6-subnets": {
      "ip_prefix": {
        "field": "ipv6",
        "prefix_length": 64,
        "is_ipv6": true
      }
    }
  }
}
```

If `is_ipv6` is `true`, the response doesn’t include a `netmask` for each bucket.

```console-result
{
  ...

  "aggregations": {
    "ipv6-subnets": {
      "buckets": [
        {
          "key": "2001:db8:a4f8:112a::",
          "is_ipv6": true,
          "doc_count": 4,
          "prefix_length": 64
        },
        {
          "key": "2001:db8:a4f8:112c::",
          "is_ipv6": true,
          "doc_count": 3,
          "prefix_length": 64
        },
        {
          "key": "2001:db8:a4f8:114f::",
          "is_ipv6": true,
          "doc_count": 2,
          "prefix_length": 64
        }
      ]
    }
  }
}
```


## Parameters [ip-prefix-agg-params]

`field`
:   (Required, string) The document IP address field to aggregate on. The field mapping type must be [`ip`](/reference/elasticsearch/mapping-reference/ip.md).

`prefix_length`
:   (Required, integer) Length of the network prefix. For IPv4 addresses, the accepted range is `[0, 32]`. For IPv6 addresses, the accepted range is `[0, 128]`.

`is_ipv6`
:   (Optional, boolean) Defines whether the prefix applies to IPv6 addresses. Just specifying the `prefix_length` parameter is not enough to know if an IP prefix applies to IPv4 or IPv6 addresses. Defaults to `false`.

`append_prefix_length`
:   (Optional, boolean) Defines whether the prefix length is appended to IP address keys in the response. Defaults to `false`.

`keyed`
:   (Optional, boolean) Defines whether buckets are returned as a hash rather than an array in the response. Defaults to `false`.

`min_doc_count`
:   (Optional, integer) Defines the minimum number of documents for buckets to be included in the response. Defaults to `1`.


## Response body [ipprefix-agg-response]

`key`
:   (string) The IPv6 or IPv4 subnet.

`prefix_length`
:   (integer) The length of the prefix used to aggregate the bucket.

`doc_count`
:   (integer) Number of documents matching a specific IP prefix.

`is_ipv6`
:   (boolean) Defines whether the netmask is an IPv6 netmask.

`netmask`
:   (string) The IPv4 netmask. If `is_ipv6` is `true` in the request, this field is missing in the response.


## Keyed Response [ipprefix-agg-keyed-response]

Set the `keyed` flag of `true` to associate an unique IP address key with each bucket and return sub-networks as a hash rather than an array.

Example:

$$$ip-prefix-keyed-example$$$

```console
GET /network-traffic/_search
{
  "size": 0,
  "aggs": {
    "ipv4-subnets": {
      "ip_prefix": {
        "field": "ipv4",
        "prefix_length": 24,
        "keyed": true
      }
    }
  }
}
```

Response:

```console-result
{
  ...

  "aggregations": {
    "ipv4-subnets": {
      "buckets": {
        "192.168.1.0": {
          "is_ipv6": false,
          "doc_count": 4,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        },
        "192.168.2.0": {
          "is_ipv6": false,
          "doc_count": 3,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        },
        "192.168.3.0": {
          "is_ipv6": false,
          "doc_count": 2,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        }
      }
    }
  }
}
```


## Append the prefix length to the IP address key [ipprefix-agg-append-prefix-length]

Set the `append_prefix_length` flag to `true` to catenate IP address keys with the prefix length of the sub-network.

Example:

$$$ip-prefix-append-prefix-len-example$$$

```console
GET /network-traffic/_search
{
  "size": 0,
  "aggs": {
    "ipv4-subnets": {
      "ip_prefix": {
        "field": "ipv4",
        "prefix_length": 24,
        "append_prefix_length": true
      }
    }
  }
}
```

Response:

```console-result
{
  ...

  "aggregations": {
    "ipv4-subnets": {
      "buckets": [
        {
          "key": "192.168.1.0/24",
          "is_ipv6": false,
          "doc_count": 4,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        },
        {
          "key": "192.168.2.0/24",
          "is_ipv6": false,
          "doc_count": 3,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        },
        {
          "key": "192.168.3.0/24",
          "is_ipv6": false,
          "doc_count": 2,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        }
      ]
    }
  }
}
```


## Minimum document count [ipprefix-agg-min-doc-count]

Use the `min_doc_count` parameter to only return buckets with a minimum number of documents.

$$$ip-prefix-min-doc-count-example$$$

```console
GET /network-traffic/_search
{
  "size": 0,
  "aggs": {
    "ipv4-subnets": {
      "ip_prefix": {
        "field": "ipv4",
        "prefix_length": 24,
        "min_doc_count": 3
      }
    }
  }
}
```

Response:

```console-result
{
  ...

  "aggregations": {
    "ipv4-subnets": {
      "buckets": [
        {
          "key": "192.168.1.0",
          "is_ipv6": false,
          "doc_count": 4,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        },
        {
          "key": "192.168.2.0",
          "is_ipv6": false,
          "doc_count": 3,
          "prefix_length": 24,
          "netmask": "255.255.255.0"
        }
      ]
    }
  }
}
```


