package testsolrj;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import junit.framework.TestCase;

public class testsolrj extends TestCase {

	@Test
	public void test1(){
		//添加索引
		try {
			System.out.println("-----------------");
			String urlString = "http://localhost:8080/solr";
			SolrServer s=new HttpSolrServer(urlString);
			SolrInputDocument document = new SolrInputDocument();
			document.addField("id","c0002");
			document.addField("product_name","solr全文索引");
			document.addField("product_price",86.5f);
			document.addField("product_picture","38.jpg");
			document.addField("product_description", "solrbook");
			document.addField("product_catalog_name", "javabook");
			
			UpdateResponse response = s.add(document);
			s.commit();
			System.out.println("success"+response);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test2(){
		//删除索引
		try {
			System.out.println("-----------------");
			String urlString = "http://localhost:8080/solr";
			SolrServer s=new HttpSolrServer(urlString);
			s.deleteById("change.me");
			s.commit();
			System.out.println("success");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test3(){
		// 连接数据库
		Connection conn;
        Statement stmt;
       ResultSet rs;
        String url = "jdbc:sqlserver://localhost:1433;DatabaseName=db_test;";
       String sql = "select * from product";
        try {
            conn = DriverManager.getConnection(url, "sa", "sa");
            // 建立Statement对象
            stmt = conn.createStatement();
            /**
             * Statement createStatement() 创建一个 Statement 对象来将 SQL 语句发送到数据库。
             */
            // 执行数据库查询语句
            rs = stmt.executeQuery(sql);
           /**
             * ResultSet executeQuery(String sql) throws SQLException 执行给定的 SQL
             * 语句，该语句返回单个 ResultSet 对象
             */
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("product_name");
              
                System.out.println("Sno:" + id + "\tSame:" + name );
            }
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (stmt != null) {
                stmt.close();
                stmt = null;
           }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("数据库连接失败");
        }
	}
	//搜索
	@Test
	public void test4(){
		// 查询对象
		try {
			String urlString = "http://localhost:8080/solr";
			SolrServer solr = new HttpSolrServer(urlString);
			SolrQuery query = new SolrQuery();
			//设置查询条件,名称“q”是固定的且必须 的
			//搜索product_keywords域，product_keywords是复制域包括product_name和product_description
			query.set("q", "product_keywords:苹果");
			// 请求查询
			QueryResponse response;
			response = solr.query(query);
			// 查询结果
			SolrDocumentList docs = response.getResults();
			// 查询文档总数
			System.out.println("查询文档总数"+docs.getNumFound());
			for (SolrDocument doc : docs) {
				//商品主键
				String id = (String) doc.getFieldValue("id");
				//商品名称Solr 全文检索服务 :
				String product_name = (String)doc.getFieldValue("product_name");
				//商品价格
				Float product_price = (Float)doc.getFieldValue("product_price");
				//商品图片
				String product_picture = (String)doc.getFieldValue("product_picture");
				//商品分类
				String product_catalog_name = (String)doc.getFieldValue("product_catalog_name");
				System.out.println("=============================");
				System.out.println("id:"+id);
				System.out.println("product_name:"+product_name);
				System.out.println("product_price:"+product_price);
				System.out.println("product_picture:"+product_picture);
				System.out.println("product_catalog_name:"+product_catalog_name);
			}
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void test5() {
		// 组合查询
		try {
			// 根据商品分类、价格范围、关键字查询，查询结果按照价格降序排序
			String urlString = "http://localhost:8080/solr";
			SolrServer s = new HttpSolrServer(urlString);
			// 查询对象
			SolrQuery query = new SolrQuery();
			// 搜索product_keywords域，product_keywords是复制域包括product_name和product_description
			// 设置商品分类、关键字查询Solr 全文检索服务 :
			query.set("q","product_keywords:苹果 and product_catalog_name:aaa");
//			query.setQuery("product_keywords:苹果  AND product_catalog_name:aaa");   
			// 设置价格范围
			query.set("fq", "product_price:[1 TO 20]");
			// 查询结果按照价格降序排序
			// query.set("sort", "product_price desc");
			query.addSort("product_price", ORDER.desc);
			// 请求查询
			QueryResponse response = s.query(query);
			// 查询结果
			SolrDocumentList docs = response.getResults();
			// 查询文档总数
			System.out.println("查询文档总数" + docs.getNumFound());
			for (SolrDocument doc : docs) {
				// 商品主键
				String id = (String) doc.getFieldValue("id");
				// 商品名称
				String product_name = (String) doc
						.getFieldValue("product_name");
				// 商品价格
				Float product_price = (Float) doc
						.getFieldValue("product_price");
				// 商品图片
				String product_picture = (String) doc
						.getFieldValue("product_picture");
				// 商品分类
				String product_catalog_name = (String) doc
						.getFieldValue("product_catalog_name");
				System.out.println("=============================");
				System.out.println("id=" + id);
				System.out.println("product_name=" + product_name);
				System.out.println("product_price=" + product_price);
				System.out.println("product_picture=" + product_picture);
				System.out.println("product_catalog_name="
						+ product_catalog_name);
			}
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void test6(){
		// 分页和高亮
		try {
			String urlString = "http://localhost:8080/solr";
			SolrServer s = new HttpSolrServer(urlString);
			// 查询对象
			SolrQuery query = new SolrQuery();
			// 设置商品分类、关键字查询
			query.setQuery("product_keywords:苹果");
			// 分页参数
			// 每页显示记录数
			int pageSize = 2;
			// 当前页码
			int curPage = 1;
			// 开始记录下标
			int begin = pageSize * (curPage - 1);
			// 起始下标
			query.setStart(begin);
			// 结束下标
			query.setRows(pageSize);
			// 设置高亮参数
			query.setHighlight(true); // 开启高亮组件
			query.addHighlightField("product_name");// 高亮字段
			query.setHighlightSimplePre("<span style='color:red'>");// 前缀标记
			query.setHighlightSimplePost("</span>");// 后缀标记
			// Solr 全文检索服务 :
			// 请求查询
			QueryResponse response = s.query(query);
			// 查询结果
			SolrDocumentList docs = response.getResults();
			// 查询文档总数
			System.out.println("查询文档总数" + docs.getNumFound());
			for (SolrDocument doc : docs) {
				// 商品主键
				String id = (String) doc.getFieldValue("id");
				// 商品名称
				String product_name = (String) doc
						.getFieldValue("product_name");
				// 商品价格
				Float product_price = (Float) doc
						.getFieldValue("product_price");
				// 商品图片
				String product_picture = (String) doc
						.getFieldValue("product_picture");
				// 商品分类
				String product_catalog_name = (String) doc
						.getFieldValue("product_catalog_name");
				System.out.println("=============================");
				System.out.println("id=" + id);
				System.out.println("product_name=" + product_name);
				System.out.println("product_price=" + product_price);
				System.out.println("product_picture=" + product_picture);
				System.out.println("product_catalog_name="
						+ product_catalog_name);
				// 高亮信息
				if (response.getHighlighting() != null) {
					if (response.getHighlighting().get(id) != null) {
						// Solr 全文检索服务 :
						Map<String, List<String>> map = response
								.getHighlighting().get(id);// 取出高亮片段
						if (map.get("product_name") != null) {
							for (String n : map.get("product_name")) {
								System.out.println(n);
							}
						}
					}
				}
			}
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
