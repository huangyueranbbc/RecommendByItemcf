package com.tianchi.hyr.recommend;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * @category 基于itemCF的商品推荐:Merchandise recommendation based on itemCF
 * @author huangyueran
 *
 */
public class StartRun {

	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "root"); // 设置权限用户
		
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://master:8020");
		config.set("yarn.resourcemanager.hostname", "master");
		// 所有mr的输入和输出目录定义在map集合中
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("Step1Input", "/user/itemcf/input/ali_t.csv"); // 数据集 datasets
		paths.put("Step1Output", "/user/itemcf/output/step1");
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "/user/itemcf/output/step2");
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output", "/user/itemcf/output/step3");
		paths.put("Step4Input1", paths.get("Step2Output"));
		paths.put("Step4Input2", paths.get("Step3Output"));
		paths.put("Step4Output", "/user/itemcf/output/step4");
		paths.put("Step5Input", paths.get("Step4Output"));
		paths.put("Step5Output", "/user/itemcf/output/step5");
		paths.put("Step6Input", paths.get("Step5Output"));
		paths.put("Step6Output", "/user/itemcf/output/step6");
//		Step1.run(config, paths);	 // 格式化 去重	Format reset
//		Step2.run(config, paths);	// 计算得分矩阵	Score matrix
//		Step3.run(config, paths);	// 计算同现矩阵	Computing co-occurrence matrix
//		Step4.run(config, paths);	// 同现矩阵和得分矩阵相乘	Multiply the co-occurrence matrix and the score matrix
//		Step5.run(config, paths);	// 把相乘之后的矩阵相加获得结果矩阵	Add the matrix after multiplication to obtain the result matrix
		Step6.run(config, paths);	// 排序推荐	Sort recommendation
	}

//	public static Map<String, Integer> R = new HashMap<String, Integer>();
//	static {
//		R.put("click", 1);
//		R.put("collect", 2);
//		R.put("cart", 3);
//		R.put("alipay", 4);
//	}
}

