# elasticsearch-custom-query-demo
This query plugin is used to overcome the complicated script query in a Vehicle trailing analysis.

The requirement is somewhat like below:
尾随分析：根据目标车辆过车的前后时间，经过的地点（可多选设备），找到目标车辆的尾随车辆。（结果返回最多1000条结果）

![image-20210218092556525](images/image-20210218092556525.png)

`实现思路`：

1) 建立如下索引结构：k1...kN为卡口ID，即DeviceID, kN对应的值为该车经过该卡口时的拍照时间，即shotTime，时间按顺序排列好，该索引可按月创建，id字段为车牌唯一ID。模拟数据如下所示：
<pre>
     {
    	"id":4,
    	"plateNo" : "car4",
    	"k1" : [
            3,
            13,
            23,
            33
            ],
    	"k2" : [
            4,
            14,
            24,
            34
            ],
    	"k6" : [
            16,
            26,
            36,
            46
            ]
    }
</pre>
2) 运用bool query 来查询经过卡口为k1,k2,k3的车辆，使用minimum_should_match来控制必须经过的卡口数量；并使用must_not排除掉被尾随车辆本身。
<pre>
{
  "query": {
    "bool": {
      "should": [
        {
          "exists": {
            "field": "k1"
          }
        },
        {
          "exists": {
            "field": "k2"
          }
        },
        {
          "exists": {
            "field": "k3"
          }
        },
        {
          "exists": {
            "field": "k4"
          }
        }
      ],
      "minimum_should_match": 3,
      "must_not": [
        {
          "term": {
            "plateNo": {
              "value": "car1"
            }
          }
        }
      ]
    }
  }
}
</pre>

3) 用script查询，来过滤数据，把满足时间间隔（如3分钟...10分钟）的数据筛选出来。
<pre>
{
  "query": {
    "bool": {
      "should": [
        {
          "exists": {
            "field": "k1"
          }
        },
        {
          "exists": {
            "field": "k2"
          }
        },
        {
          "exists": {
            "field": "k3"
          }
        },
        {
          "exists": {
            "field": "k4"
          }
        }
      ],
      "minimum_should_match": 3,
      "must_not": [
        {
          "term": {
            "plateNo": {
              "value": "car1"
            }
          }
        }
      ]
    }
  },
  "post_filter": {
    "script": {
      "script": {
        "source":"",//script to verify whether two cars' shotTime interval is less than 3 mins while two cars were captured by the same camera.
        "lang": "painless"
      }
    }
  }
}

</pre>
4) 用script的方式，需要get document by id, 存在较多性能开销，因此计划将script的逻辑用自定义query实现。也是开发该插件的初衷。

