##==================================##
## 意大利商品销售明细表             ##
##----------------------------------##
## Requirement:                     ##
## 1. 城市: 苏州                    ##
## 2. 时间范围: 前一日/前两日(周末) ##
## 3. 需汇总字段: 成交单数/总金额   ##
##----------------------------------##
## 数据筛选处理:                    ##
## 1. 排除测试账户                  ##
## 2. 城市: 苏州                    ##
## 3. 地区: 排除 木渎A              ##
## 4. 时间范围: 前一日/前两日(周末) ##
## 5. 排除订单中赠品数据            ##
## 6. 订单状态: 1.已确认; 2.已完成  ##
##==================================##

suppressPackageStartupMessages(library(RODBC))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(plyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(XLConnect))

options("scipen"=100) # 避免读取后出现科学计数法

# 用于日报数据时间点截取
start_date <- if(weekdays(today()) == "星期一"){today() - 2} else {today() - 1}
end_date <- today() - 1

# province & city control
# 按需修改
current_prov <- "江苏省"
current_city <- "苏州市"
district_excl <- "木渎A"

# test account
# 按需修改
test_account <- as.character(read.csv("test_account.csv")[,1])

con <- odbcConnect("xinyunlian", uid="analyst", pwd="123456")

#----------------------------------------------------------------------------------------

# 0. 原始数据读取与初步整理

#---------------------#
# 0-0. 意大利商品清单 #
#---------------------#
italy_prod <- read.csv("italy_prod_16_01_20.csv")                                 %>%
  mutate(sn = as.character(sn))

#-------------#
# 0-1. 地域表 #
#-------------#
# 限定省市区在已有销售记录的区域
admin_div <- sqlQuery(con, "select * from xinyunlian_area")                       %>%
  mutate(id = as.character(id),
         name = as.character(name),
         province = str_split_fixed(name_path,",",3)[,1],
         city = str_split_fixed(name_path,",",3)[,2],
         district = str_split_fixed(name_path,",",3)[,3],
         district = ifelse(province %in% c("北京市","上海市","天津市","重庆市"), city, district),
         city = ifelse(province %in% c("北京市","上海市","天津市","重庆市"), province, city),
         district = ifelse(nchar(district) < 2, NA, district),
         city = ifelse(nchar(city) < 2, NA, city))                                %>%
  select(area_id = id, province, city, district)                                  %>%
  filter(province %in% current_prov,
         city %in% current_city,
         !district %in% district_excl)                                            %>%
  filter(!is.na(district))

#-------------#
# 0-2. 订单表 #
#-------------#
order <- sqlQuery(con, paste("select id, sn as order_sn, str_to_date(create_date, '%Y-%m-%e') as create_date, str_to_date(paid_date, '%Y-%m-%e') as paid_date, order_status, member as member_id, username, area as area_id, original_amount, payment_method, shipping_method from xinyunlian_order where create_date >= '", 
                             start_date, "' and create_date <= '", end_date, " 23:59:59' and username not in (",
                             paste("'", paste(test_account, collapse = "', '"), "')", 
                                   sep = ""), " and area in (",
                             paste("'", paste(admin_div$area_id, collapse = "', '"), "')", 
                                   sep = ""), sep = ""))                          %>%
  mutate(id = as.character(id),
         order_sn = as.character(order_sn),
         member_id = as.character(member_id),
         area_id = as.character(area_id),
         username = as.character(username))

#-----------------#
# 0-3. 订单明细表 #
#-----------------#
order_item <- sqlQuery(con, paste("select str_to_date(create_date, '%Y-%m-%e') as create_date, orders as order_id, sn, product as product_id, sale_quantity, is_gift, sub_total from xinyunlian_order_item where create_date >= '",
                                  start_date, "' and create_date <= '", 
                                  end_date, " 23:59:59' and orders in (",
                                  paste("'", paste(order$id, collapse = "', '"), "')", 
                                        sep = ""), sep = ""))                     %>%
  mutate(order_id = as.character(order_id),
         sn = as.character(sn),
         product_id = as.character(product_id),
         is_gift = as.numeric(ifelse(is_gift == "\001", 1, 0)))

#-------------#
# 0-4. 客户表 #
#-------------#
# 限定省市区域
member <- sqlQuery(con, paste("select id as member_id, name, username, licence_code, shop_name, address, area as area_id, str_to_date(first_login_date, '%Y-%m-%e') as first_login_date from xinyunlian_member where login_date is not null and username not in (",
                              paste("'", paste(test_account, collapse = "', '"), 
                                    "')", sep =""), " and area in (",
                              paste("'", paste(admin_div$area_id, collapse = "', '"), "')", 
                                    sep = ""), sep = ""))                         %>%
  mutate(member_id = as.character(member_id),
         name = as.character(name),
         username = as.character(username),
         licence_code = as.character(licence_code),
         shop_name = as.character(shop_name),
         address = as.character(address),
         area_id = as.character(area_id))

#-------------#
# 0-5. 商品表 #
#-------------#
# 5列数据切换为文本向量: product_id, sn, name, cate_id (product_category), dealer_id
product <- sqlQuery(con, "select id as product_id, sn, name, brand, product_category as cate_id, 
                                 sale_factor, dealer as dealer_id, delivery_method 
                          from xinyunlian_product")                               %>%
  mutate(product_id = as.character(product_id),
         sn = as.character(sn),
         brand = as.character(brand),
         cate_id = as.character(cate_id),
         name = as.vector(name),
         dealer_id = as.character(dealer_id))

#-------------#
# 0-6. 品类表 #
#-------------#
# 4列均切换为文本向量
product_category <- sqlQuery(con, "select id as cate_id, code, name, tree_path 
                                   from xinyunlian_product_category")             %>%
  mutate(cate_id = as.character(cate_id),
         code = as.character(code),
         name = as.character(name),
         tree_path = as.character(tree_path))

# 辅助表1，第一级品类
cate1 <- product_category                                                         %>%
  filter(nchar(code) == 2)                                                        %>%
  select(cate1_id = cate_id, cate1 = name)                                        %>%
  mutate(cate1 = as.character(cate1))
# 辅助表2， 第二级品类
cate2 <- product_category                                                         %>%
  filter(nchar(code) == 4)                                                        %>%
  select(cate2_id = cate_id, cate2 = name)                                        %>%
  mutate(cate2 = as.character(cate2))

# 整合三表
prod_cate <- product_category                                                     %>%
  mutate(parent1 = str_split_fixed(tree_path,",",4)[,2],
         parent2 = str_split_fixed(tree_path,",",4)[,3])                          %>%
  left_join(cate1, by = c("parent1" = "cate1_id"))                                %>%
  left_join(cate2, by = c("parent2" = "cate2_id"))                                %>%
  select(cate_id, cate1, cate2, cate3 = name)                                     %>%
  mutate(cate1 = ifelse(is.na(cate1), cate3, cate1),
         cate2 = ifelse(is.na(cate2), cate3, cate2),
         cate3 = ifelse(cate1 == cate3, NA, cate3),
         cate3 = ifelse(cate2 == cate3, NA, cate3),
         cate2 = ifelse(cate1 == cate2, NA, cate2))

#---------------#
# 0-7. 供应商表 #
#---------------#
# 3列数据均转换为文本向量
dealer <- sqlQuery(con, "select id as dealer_id, sn, name as supplier 
                         from xinyunlian_dealer")                                 %>%
  mutate(dealer_id = as.character(dealer_id),
         sn = as.character(sn),
         supplier = as.character(supplier))

tongpei <- product                                                                %>%
  filter(delivery_method == 0)                                                    %>%
  select(sn, dealer_id)                                                           %>%
  left_join(dealer[, c("dealer_id", "supplier")], by = "dealer_id")               %>%
  mutate(method = "统配")                                                         %>%
  select(sn, supplier, method)

zipei <- product                                                                  %>%
  filter(delivery_method == 1)                                                    %>%
  select(sn, dealer_id)                                                           %>%
  left_join(dealer, by = "dealer_id")                                             %>%
  filter(dealer_id != "2")                                                        %>%
  select(sn = sn.x, supplier)                                                     %>%
  mutate(method ="自配")

supplier <- rbind(tongpei, zipei)

rm("dealer", "tongpei", "zipei")

#-----------------#
# 0-8. 上架商品表 #
#-----------------#
# 仅保留上架商品 is_marketable = 1
# 两列数据转换为文本向量
contract_product <- sqlQuery(con, paste("select area as area_id, product as product_id from xinyunlian_contract_product where is_marketable = 1 and area in (",
                                        paste("'", paste(admin_div$area_id, collapse = "', '"), "')", 
                                              sep = ""), sep = ""))               %>%
  mutate(area_id = as.character(area_id),
         product_id = as.character(product_id))

close(con)

#----------------------------------------------------------------------------------------

# 1. 数据整合
order_extracted <- order                                                          %>%
  filter(order_status %in% c(1,2,4))                                              %>%
  select(id, order_sn, member_id, create_date, username, area_id,
         original_amount, order_status)                                           %>%
  left_join(admin_div, by = "area_id")                                            %>%
  left_join(member[, c("member_id", "name", "shop_name", "address")], by = "member_id")

sales_detail <- order_item                                                        %>%
  filter(is_gift != 1)                                                            %>%
  select(order_id, sn, sale_quantity, sub_total, is_gift)                         %>%
  left_join(product[, c("sn", "cate_id", "name")], by = "sn")                     %>%
  left_join(prod_cate, by = "cate_id")                                            %>%
  left_join(supplier, by = "sn")                                                  %>%
  mutate(italy_prod = ifelse(sn %in% italy_prod$sn, 1, 0), 
         supplier = ifelse(is.na(supplier), "自营", supplier),
         method = ifelse(is.na(method), "自营", method))                          %>%
  left_join(order_extracted, by = c("order_id" = "id"))                           %>%
  select(- c(member_id, is_gift, cate_id, area_id))                               %>%
  arrange(desc(italy_prod))

sales_detail_export <- sales_detail                                               %>%
  filter(italy_prod == 1)                                                         %>%
  select(- c(supplier, method, italy_prod))                                       %>%
  select(order_id, order_sn, sn, sale_quantity, sub_total, name.x, 
         cate1, cate2, cate3, create_date, username, original_amount, 
         order_status, province, city, district, name.y, shop_name, address)

# summary_all <- sales_detail                                                       %>%
#   summarise(no_order_tt = n_distinct(order_id),
#             # quantity_tt = sum(sale_quantity),
#             subtotal_tt = sum(sub_total))

summary_italy <- sales_detail                                                     %>%
  filter(italy_prod == 1)                                                         %>%
  summarise(no_order_it = n_distinct(order_id),
            # quantity_it = sum(sale_quantity),
            subtotal_it = sum(sub_total))                                         # %>%
#   cbind(summary_all)                                                              %>%
#   mutate(order_frac = no_order_it / no_order_tt,
#          subtotal_frac = subtotal_it / subtotal_tt)

names(summary_italy) <- c("含意大利商品订单数", "订单中意大利商品总金额")

# names(summary_italy) <- c("含意大利商品订单数", "订单中意大利商品总金额",
#                           "总订单数", "订单总金额",
#                           "含意大利商品订单占比", "意大利商品金额占比")

names(sales_detail_export) <- c("数据库订单编号", "系统订单编号", "商品代码", "销售数量",
                                "商品金额", "商品名称", "一级品类", "二级品类", "三级品类", 
                                "订单创建时间", "用户编码", "订单金额", "订单状态", 
                                "省", "市", "区", "客户姓名", "店铺名称", "地址")

#----------------------------------------------------------------------------------------

# 2. 数据导出
wb <- loadWorkbook(paste("意大利商品数据_", end_date, ".xlsx", sep = ""), create = T)
createSheet(wb, "data")

writeWorksheet(wb, as.data.frame(summary_italy), sheet = "data", startCol = 1, header = T, rownames = F)
writeWorksheet(wb, as.data.frame(sales_detail_export), sheet = "data", startCol = 4, header = T, rownames = F)

saveWorkbook(wb)

