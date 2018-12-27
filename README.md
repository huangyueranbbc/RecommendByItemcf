# 基于ItemCF的协同过滤 物品推荐系统  Collaborative filtering goods recommendation system based on ItemCF  
[![Travis](https://img.shields.io/badge/RecommendByItemCF-MapReduce-green.svg)](https://github.com/huangyueranbbc/RecommendByItemcf)  [![Travis](https://img.shields.io/badge/Apache-Hadoop-ff69b4.svg)](http://hadoop.apache.org/)
Step1.run(config, paths);	 // 格式化 去重	Format reset  
Step2.run(config, paths);	// 计算得分矩阵	Score matrix  
Step3.run(config, paths);	// 计算同现矩阵	Computing co-occurrence matrix  
Step4.run(config, paths);	// 同现矩阵和得分矩阵相乘	Multiply the co-occurrence matrix and the score matrix  
Step5.run(config, paths);	// 把相乘之后的矩阵相加获得结果矩阵	Add the matrix after multiplication to obtain the result matrix  
Step6.run(config, paths);	// 排序推荐	Sort recommendation  
