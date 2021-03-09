package cache

import "strings"

const keySeparator = ","

// TotalWeights returns the total weights of given nodes.
// 计算ClusterConf的总权重
func TotalWeights(c []NodeConf) int {
	var weights int

	for _, node := range c {
		if node.Weight < 0 {
			node.Weight = 0
		}
		weights += node.Weight
	}

	return weights
}

// 将keys转换成以逗号间隔的字符串
func formatKeys(keys []string) string {
	return strings.Join(keys, keySeparator)
}
