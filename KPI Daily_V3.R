suppressPackageStartupMessages(library(RODBC))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(plyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(XLConnect))

options("scipen"=100) # 避免读取后出现科学计数法

#--------------#
# 初始参数设置 #
#--------------#
# date control

# 用于日报数据(上午九点)
report_date <- today()-1
before_a_week_date <- today()-8
before_a_month_date <- today()-31

# 用于下午五点数据抓取
report_date <- today()
before_a_week_date <- today()-7
before_a_month_date <- today()-30

# province & city control
# 按需修改
current_prov <- c("浙江省", "江苏省")
current_city <- c("苏州市", "杭州市", "南京市", "嘉兴市")
district_excl <- c("木渎A", "海盐A", "海宁A")

# test account
# 按需修改
test_account <- as.character(read.csv("test_account.csv")[,1])

con <- odbcConnect("xinyunlian", uid="analyst", pwd="123456")

#-------------#
# 0-1. 订单表 #
#-------------#
# 转换日期格式
# 将4种id换成文本格式：id, member, area, username
# 删除测试账号
# 删除海宁市(area_id == "970")中username以"X"结尾的账号
# 删除木渎A、海盐A和海宁A

# !!!!!
# 不可设置时间筛选条件
# 因为后期处理要查看完整数据
# 考虑相关后期处理直接转换成mysql代码在数据库中完成
order <- sqlQuery(con, paste("select id, str_to_date(create_date, '%Y-%m-%e') as create_date, str_to_date(paid_date, '%Y-%m-%e') as paid_date, str_to_date(returns_date, '%Y-%m-%e') as returns_date, order_status, member as member_id, username, area as area_id, original_amount, payment_method, shipping_method, shipping_status, delivery_method from xinyunlian_order where username not in (",
                             paste("'", paste(test_account, collapse = "', '"), "')", 
                                   sep = "")))                                    %>%
  mutate(id = as.character(id),
         member_id = as.character(member_id),
         area_id = as.character(area_id),
         username = as.character(username))                                       %>%
  filter(!(area_id == "970" & str_detect(username, "X$")),
         !area_id %in% c("3325", "3329", "3313"))

# order <- sqlQuery(con, paste("select id, str_to_date(create_date, '%Y-%m-%e') as create_date, str_to_date(paid_date, '%Y-%m-%e') as paid_date, str_to_date(returns_date, '%Y-%m-%e') as returns_date, order_status, member as member_id, username, area as area_id, original_amount, payment_method, shipping_method from xinyunlian_order where create_date > '", 
#                              before_a_month_date, "' and create_date <= '", report_date, "' and username not in (",
#                              paste("'", paste(test_account, collapse = "', '"), "')", 
#                                    sep = "")))                                    %>%
#   mutate(id = as.character(id),
#          member_id = as.character(member_id),
#          area_id = as.character(area_id),
#          username = as.character(username))                                       %>%
#   filter(!(area_id == "970" & str_detect(username, "X$")))

#-----------------#
# 0-2. 订单明细表 #
#-----------------#
# 筛选日期为昨日至31日前，即以昨日为起点，动态的30日订单情况
# 将3种id换成文本格式: order_id, sn, product_id
# is_gift: 将数值换为0或1
# 筛选: order_id 存在于订单表的id列中
order_item <- sqlQuery(con, paste("select str_to_date(create_date, '%Y-%m-%e') as create_date, orders as order_id, sn, product as product_id, quantity, sale_factor, sale_quantity, is_gift, sub_total from xinyunlian_order_item where create_date > '",
                       before_a_month_date, "' and create_date <= '", 
                       report_date, " 23:59:59'", sep = ""))                      %>%
  mutate(order_id = as.character(order_id),
         sale_quantity = quantity / sale_factor,
         sn = as.character(sn),
         product_id = as.character(product_id),
         is_gift = as.numeric(ifelse(is_gift == "\001", 1, 0)))                   %>%
  filter(order_id %in% order$id,
         is_gift == 0)                                                            %>%
  select(-c(quantity, sale_factor))

#-----------------#
# 0-3. 订单日志表 #
#-----------------#
# 筛选日期为昨日至31日前，即以昨日为起点，动态的30日订单情况
# 将1种id换成文本格式：order_id
# 筛选: order_id 存在于订单表的id列中
order_log <- sqlQuery(con, paste("select str_to_date(create_date, '%Y-%m-%e') as create_date, type, orders as order_id from xinyunlian_order_log where create_date > '",
                                 before_a_month_date, "' and create_date <= '", 
                                 report_date, " 23:59:59'", sep = ""))            %>%
  mutate(order_id = as.character(order_id))                                       %>%
  filter(order_id %in% order$id)

#-------------#
# 0-4. 地域表 #
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
         !district %in% district_excl)

#-------------#
# 0-5. 客户表 #
#-------------#
# 限定省市区域
area_id <- paste("'",paste(admin_div$area_id, collapse = "', '"), "'", sep = "")

member <- sqlQuery(con, paste("select id as member_id, name, username, licence_code, shop_name, address, area as area_id, str_to_date(first_login_date, '%Y-%m-%e') as first_login_date from xinyunlian_member where area in (", 
                              area_id, ") and username not in (", 
                              paste("'", paste(test_account, collapse = "', '"), 
                                    "')", sep =""), sep = ""))                    %>%
  mutate(member_id = as.character(member_id),
         name = as.character(name),
         username = as.character(username),
         licence_code = as.character(licence_code),
         shop_name = as.character(shop_name),
         address = as.character(address),
         area_id = as.character(area_id))
#  filter(!str_detect(username, "XSM"), !str_detect(username, "XYL"),
#         !str_detect(username, "JXS"), !str_detect(username, "123456"),
#         !str_detect(username, "987654"), !str_detect(username, "000000"),
#         !str_detect(username, "999999"), !str_detect(username, "222222"),
#         !str_detect(username, "111111111111"))

##---------------#
# 0-6. app登录表 #
#----------------#
# 仅保留 method_name == "userLogin",
# 仅保留 substr(snap_shot, 1, 10) == '{"code":1,'
# member_id 换成文本格式
# platform_type 换为 文本向量
app <- sqlQuery(con, paste("select str_to_date(create_date, '%Y-%m-%e') as login_date, operator as member_id, come_from as platform_type from xinyunlian_app_log where method_name = 'userLogin' and substring(snap_shot, 1, 10) = '{", 
                           '"code":1,',"'", sep = ""))                            %>%
  mutate(member_id = as.character(member_id),
         platform_type = as.vector(platform_type))

#-------------#
# 0-7. 商品表 #
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
# 0-8. 品类表 #
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

# rm("product_category", "cate1", "cate2")

#---------------#
# 0-9. 供应商表 #
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

#------------------#
# 0-10. 上架商品表 #
#------------------#
# 仅保留上架商品 is_marketable = 1
# 两列数据转换为文本向量
contract_product <- sqlQuery(con, "select area as area_id, product as product_id 
                                   from xinyunlian_contract_product
                                   where is_marketable = 1")                      %>%
  mutate(area_id = as.character(area_id),
         product_id = as.character(product_id))

#--------------#
# 0-11. 登陆表 #
#--------------#
# 筛选固定市区
# 筛除测试账号
# 前四列均转换为文本向量
login <- sqlQuery(con, paste("select member as member_id, username, area as area_id, login_ip, str_to_date(login_date, '%Y-%m-%e') as login_date, str_to_date(first_login_date, '%Y-%m-%e') as first_login_date from xinyunlian_member_login_history where area in (", 
                             area_id, ") and username not in (", paste("'", paste(test_account, collapse = "', '"), sep =""), "') and login_date > '", before_a_month_date, "' and login_date <= '", report_date,
                             " 23:59:59'", sep = ""))                                      %>%
  mutate(member_id = as.character(member_id),
         username = as.character(username),
         area_id = as.character(area_id),
         login_ip = as.character(login_ip))

# login <- sqlQuery(con, paste("select member as member_id, username, area as area_id, login_ip, str_to_date(login_date, '%Y-%m-%e %H:%i:%s') as login_date, str_to_date(first_login_date, '%Y-%m-%e %H:%i:%s') as first_login_date from xinyunlian_member_login_history where area in (", 
#                              area_id, ") and username not in (", paste("'", paste(test_account, collapse = "', '"), sep =""), "') and login_date > '", before_a_month_date,
#                              "'", sep = ""))                                      %>%
#   mutate(member_id = as.character(member_id),
#          username = as.character(username),
#          area_id = as.character(area_id),
#          login_ip = as.character(login_ip))

close(con)

#===============================================================================================

#-----------------#
# 0. 首次购买人数 #
#-----------------#
distinct_purchaser <- order                                                       %>%
  select(username, create_date, paid_date)                                        %>%
  group_by(username)                                                              %>%
  summarise(first_paid_date = first(paid_date))

#-----------------------------#
# 1. 首次登录数、首次购买人数 #
#-----------------------------#
# APP首次登陆
app_login <- app                                                                  %>%
  arrange(login_date)                                                             %>%
  group_by(member_id)                                                             %>%
  summarise(first_login_date = first(login_date))                                 %>%
  left_join(member[, c("member_id", "area_id")], by = "member_id")

# PC首次登陆
pc_login <- member                                                                %>%
  select(member_id, first_login_date, area_id)

# 整合新增登录数: 排除app/pc重复项
first_login <- rbind(app_login, pc_login)                                         %>%
  left_join(admin_div, by = "area_id")                                            %>%
  filter(!is.na(city))                                                            %>%
  arrange(first_login_date)                                                       %>%
  group_by(city, member_id)                                                       %>%
  summarise(first_login_date = first(first_login_date))                           %>%
  group_by(city, first_login_date)                                                %>%
  summarise(count = n())                                                          %>%
  filter(first_login_date > before_a_week_date,
         first_login_date <= report_date)                                         %>%
  rename(date = first_login_date)

# 新增购买人数: 
# check the first_paid_date of each client (grouped by username)
# then count by date the number of client
first_order <- order                                                              %>%
  filter(order_status %in% 1:2)                                                   %>%
  select(area_id, username, create_date, paid_date)                               %>%
  left_join(admin_div, by = "area_id")                                            %>%
  filter(!is.na(city))                                                            %>%
  group_by(city, username)                                                        %>%
  summarise(first_paid_date = first(paid_date))                                   %>%
  group_by(city, first_paid_date)                                                 %>%
  summarise(count = n())                                                          %>%
  filter(first_paid_date > before_a_week_date,
         first_paid_date <= report_date)                                          %>%
  rename(date = first_paid_date)

# 避免出现无销量便无对应记录的情况，
# 预先设定必须出现的城市和日期区间
city_date_range <- expand.grid(current_city, 
                               seq(before_a_week_date + 1, report_date, 1))       %>%
  rename(city = Var1, date = Var2)                                                %>%
  mutate(city = as.vector(city))                                                  %>%
  arrange(city, date)

city_login_order <- left_join(city_date_range, first_login, c("city", "date"))    %>%
  left_join(first_order, c("city", "date"))                                       %>%
  rename(cnt_login = count.x, cnt_order = count.y)                                %>%
  as.data.frame()                                                                 %>%
  mutate(cnt_login = ifelse(is.na(cnt_login), 0, cnt_login),
         cnt_order = ifelse(is.na(cnt_order), 0, cnt_order))                      %>%
  as.data.frame()

all_login_order <- city_login_order                                               %>%
  select(date, cnt_login, cnt_order)                                              %>%
  group_by(date)                                                                  %>%
  summarise(cnt_login = sum(cnt_login), cnt_order = sum(cnt_order))               %>%
  mutate(city = "各地汇总")          %>%
  as.data.frame()

table1 <- union(city_login_order, all_login_order)                                %>%
  group_by(city)                                                                  %>%
  mutate(login_max = ifelse(cnt_login == max(cnt_login), cnt_login, NA),
         login_min = ifelse(cnt_login == min(cnt_login), cnt_login, NA),
         order_max = ifelse(cnt_order == max(cnt_order), cnt_order, NA),
         order_min = ifelse(cnt_order == min(cnt_order), cnt_order, NA))          %>%
  arrange(city, date)                                                             %>%
  group_by(city)                                                                  %>%
  mutate(login_avg = mean(cnt_login), order_avg = mean(cnt_order))

write.csv(table1, "table1.csv", row.names = F)

#---------------#
# 2. 访问及订单 #
#---------------#

# 订单整合  
# order_status = 3 为已取消订单
# returns_date 非空计为退货订单
# 仅保留三市
# 排除农夫专项
order_extracted <- order                                                          %>%
  filter(order_status %in% c(1,2,4),
         create_date > before_a_week_date,
         create_date <= report_date)                                              %>%
  select(id, member_id, create_date, username, area_id, returns_date,
         original_amount, order_status)                                           %>%
  left_join(admin_div, by = "area_id")                                            %>%
  filter(!(district == "海宁市" & str_detect(username, "X$")))

# 访问汇总
# 移除山东省数据
login_extracted <- login                                                          %>%
  filter(login_date > before_a_week_date,
         login_date <= report_date)                                               %>%
  select(member_id, username, area_id, login_ip, login_date)                      %>%
  left_join(admin_div, by = "area_id")                                            %>%
  filter(!(district == "海宁市" & str_detect(username, "X$")))

# 三区访问量
# 三区总额，排除取消订单及退单
# 总数/按市/按行政区划
# ****    ****
city_district_range <- admin_div                                                  %>%
  filter(province %in% current_prov,
         city %in% current_city,
         !district %in% district_excl,
         !is.na(district))                                                        %>%
  select(city, district)

## 日访问
total_visit <- login_extracted                                                    %>%
  filter(login_date == report_date)                                               %>%
  summarise(total_visitors = n(), unique_visitors = n_distinct(member_id))

city_visit <- login_extracted                                                     %>%
  filter(login_date == report_date)                                               %>%
  group_by(city)                                                                  %>%
  summarise(total_visitors = n(), unique_visitors = n_distinct(member_id))

district_visit <- login_extracted                                                 %>%
  filter(login_date == report_date)                                               %>%
  group_by(city, district)                                                        %>%
  summarise(total_visitors = n(), unique_visitors = n_distinct(member_id))

## 日订单
total_sku_amount <- order_extracted                                               %>%
  filter(create_date == report_date)                                              %>%
  summarise(people_count = n_distinct(username), total_quant = n(), 
            total_amount = sum(original_amount), 
            daily_pct = total_amount / n())

city_sku_amount <- order_extracted                                                %>%
  filter(create_date == report_date)                                              %>%
  group_by(city)                                                                  %>%
  summarise(people_count = n_distinct(username), total_quant = n(), 
            total_amount = sum(original_amount), 
            daily_pct = total_amount / n())

district_sku_amount <- order_extracted                                            %>%
  filter(create_date == report_date)                                              %>%
  group_by(city, district)                                                        %>%
  summarise(people_count = n_distinct(username), total_quant = n(), 
            total_amount = sum(original_amount), 
            daily_pct = total_amount / n())

## 周订单
total_wkly_amount <- order_extracted                                              %>%
  summarise(wkly_total_amount = sum(original_amount),
            wkly_pct = wkly_total_amount / n(), wkly_avg_quant = n() / 7)

city_wkly_amount <- order_extracted                                               %>%
  group_by(city)                                                                  %>%
  summarise(wkly_total_amount = sum(original_amount),
            wkly_pct = wkly_total_amount / n(), wkly_avg_quant = n() / 7)

district_wkly_amount <- order_extracted                                           %>%
  group_by(city, district)                                                        %>%
  summarise(wkly_total_amount = sum(original_amount),
            wkly_pct = wkly_total_amount / n(), wkly_avg_quant = n() / 7)

## 重复访问率
total_dup_visit <- login_extracted                                                %>%
  group_by(username)                                                              %>%
  summarise(visit_count = n_distinct(login_date))                                 %>%
  summarise(login_count = n(), dup_login_count = sum(visit_count >= 2),
            dup_login_rate = dup_login_count / login_count)

city_dup_visit <- login_extracted                                                 %>%
  group_by(city, username)                                                        %>%
  summarise(visit_count = n_distinct(login_date))                                 %>%
  summarise(login_count = n(), dup_login_count = sum(visit_count >= 2),
            dup_login_rate = dup_login_count / login_count)

district_dup_visit <- login_extracted                                             %>%
  group_by(city, district, username)                                              %>%
  summarise(visit_count = n_distinct(login_date))                                 %>%
  summarise(login_count = n(), dup_login_count = sum(visit_count >= 2),
            dup_login_rate = dup_login_count / login_count)

## 重复订单率
total_dup_order <- order_extracted                                                %>%
  group_by(username)                                                              %>%
  summarise(order_cnt = n_distinct(create_date))                                  %>%
  summarise(order_count = n(), dup_order_count = sum(order_cnt >= 2),
            dup_order_rate = dup_order_count / order_count)

city_dup_order <- order_extracted                                                 %>%
  group_by(city, username)                                                        %>%
  summarise(order_cnt = n_distinct(create_date))                                  %>%
  summarise(order_count = n(), dup_order_count = sum(order_cnt >= 2),
            dup_order_rate = dup_order_count / order_count)

district_dup_order <- order_extracted                                             %>%
  group_by(city, district, username)                                              %>%
  summarise(order_cnt = n_distinct(create_date))                                  %>%
  summarise(order_count = n(), dup_order_count = sum(order_cnt >= 2),
            dup_order_rate = dup_order_count / order_count)


total_visit_order <- cbind(total_visit, total_sku_amount, total_wkly_amount,
                           total_dup_visit, total_dup_order)                      %>% 
  mutate(city = NA, district = NA)                                                %>%
  select(city, district, total_visitors, unique_visitors, people_count, 
         total_quant, total_amount, login_count, dup_login_count, 
         dup_login_rate, order_count, dup_order_count, dup_order_rate,
         daily_pct, wkly_pct, wkly_avg_quant)

city_visit_order <- left_join(city_visit, city_sku_amount, by = "city")           %>%
  left_join(city_wkly_amount, by = "city")                                        %>%
  left_join(city_dup_visit, by = "city")                                          %>%
  left_join(city_dup_order, by = "city")                                          %>%
  mutate(district = NA)                                                           %>%
  select(city, district, total_visitors, unique_visitors, people_count, 
         total_quant, total_amount, login_count, dup_login_count, 
         dup_login_rate, order_count, dup_order_count, dup_order_rate,
         daily_pct, wkly_pct, wkly_avg_quant)

district_visit_order <- left_join(district_visit, district_sku_amount, 
                                  by = c("city", "district"))                     %>%
  left_join(district_wkly_amount, by = c("city", "district"))                     %>%
  left_join(district_dup_visit, by = c("city", "district"))                       %>%
  left_join(district_dup_order, by = c("city", "district"))                       %>%
  select(city, district, total_visitors, unique_visitors, people_count, 
         total_quant, total_amount, login_count, dup_login_count, 
         dup_login_rate, order_count, dup_order_count, dup_order_rate,
         daily_pct, wkly_pct, wkly_avg_quant)                                     # %>%
  # right_join(city_district_range, by = c("city", "district"))

table2 <- rbind(total_visit_order, city_visit_order, district_visit_order)        %>%
  mutate(district = ifelse(is.na(city) & is.na(district), "各地汇总",
                           ifelse(is.na(district), city, district)),
         city = ifelse(is.na(city), "汇总", city),
         total_visitors = ifelse(is.na(total_visitors), 0, total_visitors),
         unique_visitors = ifelse(is.na(unique_visitors), 0, unique_visitors),
         people_count = ifelse(is.na(people_count), 0, people_count),
         total_quant =  ifelse(is.na(total_quant), 0, total_quant),
         total_amount =  ifelse(is.na(total_amount), 0, total_amount),
         conv_rate = ifelse(unique_visitors == 0, "#N/A", people_count/unique_visitors),
         login_count =  ifelse(is.na(login_count), 0, login_count),
         dup_login_count =  ifelse(is.na(dup_login_count), 0, dup_login_count),
         order_count =  ifelse(is.na(order_count), 0, order_count),
         dup_order_count =  ifelse(is.na(dup_order_count), 0, dup_order_count),
         daily_pct =  ifelse(is.na(daily_pct), 0, daily_pct),
         wkly_pct =  ifelse(is.na(wkly_pct), 0, wkly_pct),
         wkly_avg_quant =  ifelse(is.na(wkly_avg_quant), 0, wkly_avg_quant))      %>%
  select(city, district, total_visitors, unique_visitors, people_count, 
         total_quant, total_amount, conv_rate, login_count, dup_login_count, 
         dup_login_rate, order_count, dup_order_count, dup_order_rate,
         daily_pct, wkly_pct, wkly_avg_quant)

write.csv(table2, "table2.csv", row.names = F)

#-----------#
# 3. 动销表 #
#-----------#

# 3-0-2 商品品类表


# 3 进销存表 
sales_detail <- order_item                                                        %>%
  filter(create_date > before_a_week_date,
         create_date <= report_date,
         order_id %in% order$id, is_gift != 1)                                    %>%
  select(order_id, sn, sale_quantity, sub_total, is_gift)                         %>%
  left_join(product[, c("sn", "cate_id")], by = "sn")                             %>%
  left_join(prod_cate, by = "cate_id")                                            %>%
  left_join(supplier, by = "sn")                                                  %>%
  mutate(supplier = ifelse(is.na(supplier), "自营", supplier),
         method = ifelse(is.na(method), "自营", method))                          %>%
  left_join(order_extracted, by = c("order_id" = "id"))                           %>%
  select(- c(member_id, returns_date))
  
# 避免出现无销量便无对应记录的情况，
# 预先设定必须出现的城市、日期区间及配送方式
city_date_alloc <- expand.grid(current_city, 
                               seq(before_a_week_date + 1, report_date, 1),
                               c("统配", "自配", "自营"))                         %>%
  rename(city = Var1, create_date = Var2, method =Var3)                           %>%
  mutate(city = as.vector(city),
         method = as.vector(method))                                              %>%
  arrange(city, create_date, method)
  
city_sales_method <- sales_detail                                                 %>%
  group_by(city, create_date, method)                                             %>%
  summarise(amount = sum(sub_total))                                              %>% # 总额
  right_join(city_date_alloc, by = c("city", "create_date", "method"))            %>%
  spread(method, amount)                                                          %>%
  rename(unified_alloc = 统配, self_alloc = 自配, self_supp = 自营, date = create_date)

# 避免出现无销量便无对应记录的情况，
# 预先设定必须出现的城市、日期区间及一级品类
city_date_cate1 <- expand.grid(current_city, 
                               seq(before_a_week_date + 1, report_date, 1),
                               cate1$cate1)                                       %>%
  rename(city = Var1, date = Var2, cate1 =Var3)                                   %>%
  mutate(city = as.vector(city),
         cate1 = as.vector(cate1))                                                %>%
  arrange(city, date, cate1)

# 自营商品各品类销量
city_sales_cate_self_supp <- sales_detail                                         %>%
  filter(method == "自营")                                                        %>%
  group_by(city, create_date, cate1)                                              %>%
  rename(date = create_date)                                                      %>%
  summarise(amount = sum(sub_total))                                              %>%
  right_join(city_date_cate1, by = c("city", "date", "cate1"))                    %>%
  spread(cate1, amount)                                                           %>%
  rename(house_care_ss = 家庭清洁, imports_ss = 进口商品, drinks_ss = 酒水饮料,
         grain_oil_ss = 粮油副食, hair_care_ss = 美容洗护, general_merch_ss = 日用百货,
         dairy_ss = 乳品冲调, snacks_ss = 休闲零食)

# 平台商品各品类销量
city_sales_cate_platform <- sales_detail                                          %>%
  filter(method != "自营")                                                        %>%
  group_by(city, create_date, cate1)                                              %>%
  rename(date = create_date)                                                      %>%
  summarise(amount = sum(sub_total))                                              %>%
  right_join(city_date_cate1, by = c("city", "date", "cate1"))                    %>%
  spread(cate1, amount)                                                           %>%
  rename(house_care_pf = 家庭清洁, imports_pf = 进口商品, drinks_pf = 酒水饮料,
         grain_oil_pf = 粮油副食, hair_care_pf = 美容洗护, general_merch_pf = 日用百货,
         dairy_pf = 乳品冲调, snacks_pf = 休闲零食)

# 避免出现无销量便无对应记录的情况，
# 预先设定必须出现的城市和日期区间
city_date_range <- expand.grid(current_city, 
                               seq(before_a_week_date + 1, report_date, 1))       %>%
  rename(city = Var1, date = Var2)                                                %>%
  mutate(city = as.vector(city))                                                  %>%
  arrange(city, date)

city_sales_total <- city_date_range                                               %>%
  left_join(city_sales_method , by = c("city", "date"))                           %>%
  left_join(city_sales_cate_self_supp, by = c("city", "date"))                    %>%
  left_join(city_sales_cate_platform, by = c("city", "date"))                     %>%
  mutate(unified_alloc = ifelse(is.na(unified_alloc), 0, unified_alloc),
         self_alloc = ifelse(is.na(self_alloc), 0, self_alloc),
         self_supp = ifelse(is.na(self_supp), 0, self_supp),
         house_care_ss = ifelse(is.na(house_care_ss), 0, house_care_ss),
         imports_ss = ifelse(is.na(imports_ss), 0, imports_ss),
         drinks_ss = ifelse(is.na(drinks_ss), 0, drinks_ss),
         grain_oil_ss = ifelse(is.na(grain_oil_ss), 0, grain_oil_ss),
         hair_care_ss = ifelse(is.na(hair_care_ss), 0, hair_care_ss),
         general_merch_ss = ifelse(is.na(general_merch_ss), 0, general_merch_ss),
         dairy_ss = ifelse(is.na(dairy_ss), 0, dairy_ss),
         snacks_ss = ifelse(is.na(snacks_ss), 0, snacks_ss),
         house_care_pf = ifelse(is.na(house_care_pf), 0, house_care_pf),
         imports_pf = ifelse(is.na(imports_pf), 0, imports_pf),
         drinks_pf = ifelse(is.na(drinks_pf), 0, drinks_pf),
         grain_oil_pf = ifelse(is.na(grain_oil_pf), 0, grain_oil_pf),
         hair_care_pf = ifelse(is.na(hair_care_pf), 0, hair_care_pf),
         general_merch_pf = ifelse(is.na(general_merch_pf), 0, general_merch_pf),
         dairy_pf = ifelse(is.na(dairy_pf), 0, dairy_pf),
         snacks_pf = ifelse(is.na(snacks_pf), 0, snacks_pf),
         total_amount = unified_alloc + self_alloc + self_supp)                   %>%
  arrange(city, date)

sales_total <- city_sales_total                                                   %>%
  group_by(date)                                                                  %>%
  summarise(unified_alloc = sum(unified_alloc),
            self_alloc = sum(self_alloc),
            self_supp = sum(self_supp),
            house_care_ss = sum(house_care_ss),
            imports_ss = sum(imports_ss),
            drinks_ss = sum(drinks_ss),
            grain_oil_ss = sum(grain_oil_ss),
            hair_care_ss = sum(hair_care_ss),
            general_merch_ss = sum(general_merch_ss),
            dairy_ss = sum(dairy_ss),
            snacks_ss = sum(snacks_ss),
            house_care_pf = sum(house_care_pf),
            imports_pf = sum(imports_pf),
            drinks_pf = sum(drinks_pf),
            grain_oil_pf = sum(grain_oil_pf),
            hair_care_pf = sum(hair_care_pf),
            general_merch_pf = sum(general_merch_pf),
            dairy_pf = sum(dairy_pf),
            snacks_pf = sum(snacks_pf),
            total_amount = sum(total_amount))                                     %>%
  mutate(city = "各地汇总")                                                       %>%
  select(city, date, total_amount, unified_alloc, self_alloc, self_supp, 
         house_care_ss, imports_ss, drinks_ss, grain_oil_ss, 
         hair_care_ss, general_merch_ss, dairy_ss, snacks_ss,
         house_care_pf, imports_pf, drinks_pf, grain_oil_pf, 
         hair_care_pf, general_merch_pf, dairy_pf, snacks_pf)

table3 <- rbind(sales_total, city_sales_total)
write.csv(table3, "table3.csv", row.names = F)

#-----------#
# 4. 物流表 #
#-----------#

#发货时间
shippingdate <- order_log                                                         %>%
  filter(type == 15)                                                              %>% # 15 - 已发货
  select(shipping_date = create_date, order_id)

#完成时间
completedate <- order_log                                                         %>%
  filter(type == 18)                                                              %>% # 18 - 已完成
  select(complete_date = create_date, order_id)

# 物流详细表
delivery_order <- order                                                           %>%
  select(id, create_date, username, order_status, shipping_status, area_id, 
         delivery_method)                                                         %>%
  filter(create_date > before_a_week_date,
         create_date <= report_date,
         order_status %in% 1:2,                                                       # 1 - 已确认; 2 - 已完成
         shipping_status != 0, shipping_status != 6)                              %>% # 0 - 未发货; 6 - 待发货
  left_join(shippingdate, by = c("id"="order_id"))                                %>%
  left_join(completedate, by = c("id"="order_id"))                                %>%
  mutate(duration = ifelse(is.na(shipping_date) & !is.na(complete_date), # 若发货日期缺失，则 完成时间-订单创建时间
                           as.numeric(difftime(complete_date, create_date, units = "days")), 
                           as.numeric(difftime(complete_date, shipping_date, units = "days"))),
         delivery_effect = ifelse(is.na(complete_date) & !is.na(shipping_date), "在途",
                                  ifelse(duration <= 1, "1天",
                                         ifelse(duration <= 2, "2天",
                                                ifelse(duration <= 3, "3天", "3天以上")))),
         ship_status_simple = ifelse(shipping_status %in% c(4, 7), "returned",
                                     ifelse(shipping_status == 8, 
                                            "refused", "normal")))                %>%
  left_join(admin_div, by = "area_id")

# 避免出现无销量便无对应记录的情况，
# 预先设定必须出现的城市， 配送时效 和 配送状态
city_delivery_status <- expand.grid(current_city, 
                                    c("1天", "2天", 
                                      "3天", "3天以上", "在途"),
                                    c("returned", "refused", "normal"))           %>%
  rename(city = Var1, delivery_effect = Var2, ship_status_simple = Var3)          %>%
  mutate(city = as.character(city),
         delivery_effect = as.character(delivery_effect),
         ship_status_simple = as.character(ship_status_simple))                   %>%
  arrange(city, delivery_effect, ship_status_simple)

# 4-1 订单最终状态分类
delivery_summary1 <- delivery_order                                               %>%
  group_by(city, delivery_effect, ship_status_simple)                             %>%
  summarise(count = n())                                                          %>%
  right_join(city_delivery_status, 
             by = c("city", "delivery_effect", "ship_status_simple"))             %>%
  spread(ship_status_simple, count)                                               %>%
  as.data.frame()                                                                 %>%
  mutate(returned = ifelse(is.na(returned), 0, returned),
         refused = ifelse(is.na(refused), 0, refused),
         normal = ifelse(is.na(normal), 0, normal))                               %>%
  arrange(city, delivery_effect)

# 避免出现无销量便无对应记录的情况，
# 预先设定必须出现的 城市， 配送时效 和 配送方式
city_delivery_alloc <- expand.grid(current_city, 
                                   c("1天", "2天", 
                                     "3天", "3天以上", "在途"),
                                   c("unified_alloc", "self_alloc", 
                                     "self_supp"))                                %>%
  rename(city = Var1, delivery_effect = Var2, method = Var3)                      %>%
  mutate(city = as.character(city),
         delivery_effect = as.character(delivery_effect),
         method = as.character(method))                                           %>%
  arrange(city, delivery_effect, method)

# 4-2 配送方式分类
delivery_summary2 <- delivery_order                                               %>%
  filter(ship_status_simple == "normal")                                          %>%
  mutate(method = ifelse(delivery_method == 0, "unified_alloc",
                         ifelse(delivery_method == 1, "self_alloc", "self_supp")))%>%
  group_by(city, delivery_effect, method)                                         %>%
  summarise(count = n())                                                          %>%
  right_join(city_delivery_alloc, 
             by = c("city", "delivery_effect", "method"))                         %>%
  spread(method, count)                                                           %>%
  right_join(distinct(city_delivery_status[,1:2]), 
             by = c("city", "delivery_effect"))                                   %>%
  as.data.frame()                                                                 %>%
  mutate(unified_alloc = ifelse(is.na(unified_alloc), 0, unified_alloc),
         self_alloc = ifelse(is.na(self_alloc), 0, self_alloc),
         self_supp = ifelse(is.na(self_supp), 0, self_supp))                      %>%
  arrange(city, delivery_effect)

city_deliery_summary <- left_join(delivery_summary1, delivery_summary2, 
                                  by = c("city", "delivery_effect"))              %>%
  select(city, delivery_effect, normal, refused, returned,
         self_alloc, unified_alloc, self_supp)

# 总数
delivery_summary <- left_join(delivery_summary1, delivery_summary2, 
                              by = c("city", "delivery_effect"))                  %>%
  group_by(delivery_effect)                                                       %>%
  summarise(normal = sum(normal),
            refused = sum(refused),
            returned = sum(returned),
            self_alloc = sum(self_alloc),
            self_supp = sum(self_supp),
            unified_alloc = sum(unified_alloc))                                   %>%
  mutate(city = "各地汇总")                                                       %>%
  select(city, delivery_effect, normal, refused, returned,
         self_alloc, unified_alloc, self_supp)
  
# table4
table4 <- rbind(delivery_summary, city_deliery_summary)
write.csv(table4, "table4.csv", row.names = F)


#-----------------#
# 5. 其它补充报表 #
#-----------------#

# 5-1 top商品排行
# 一个月内所售商品清单
#-----------------------------------------------------------------------------#
# 筛选条件
# 1. 非测试账号、团购账号 (orders %in% order$id, order表在读取时删除此类账号)
# 2. 非赠品 (is_gift != "\001")
# 3. 订单日期，最近三十天
# 4. 农夫项目 (木渎A; 海盐A; 海宁, 客户编号带X的)
# 
# 计算
# 1. 箱数 = quantity / sale_factor
#
# 注意
# 一个品名可能对应多个sn，最终以品名为标准进行统计
#-----------------------------------------------------------------------------#

# 最近一个月商品动销详细记录
top_ordered <- order_item                                                         %>%
  select(create_date, sn, product_id, quantity = sale_quantity, 
         sub_total, order_id)                                                     %>%
  left_join(product[, c("sn", "name")], by = "sn")                                %>%
  filter(create_date > before_a_month_date,
         create_date <= report_date)                                              %>%
  left_join(order[,c("id", "order_status", "username", "area_id")], 
            by = c("order_id" = "id"))                                            %>%
  filter(order_status %in% 1:2)                                                   %>%
  left_join(supplier[, -2], "sn")                                                 %>% # 移除供应商名称
  left_join(admin_div[, -2], by = "area_id")                                      %>%
  select(-product_id, -area_id, -order_status)                                    %>%
  mutate(method = ifelse(is.na(method), "自营", method))

# 当月销量
month_sold <- top_ordered                                                         %>%
  group_by(city, name)                                                            %>%
  summarise(monthly_quantity = sum(quantity))
  
# 当日排名
today_sold <- top_ordered                                                         %>%
  filter(create_date == report_date)                                              %>%
  group_by(city, name)                                                            %>%
  summarise(today_quantity = sum(quantity))                                       %>%
  arrange(desc(today_quantity))                                                   %>%
  slice(1:20)                                                                     %>%
  left_join(month_sold, by = c("city", "name"))
  
  
# write.csv(today_sold, "城市畅销商品top20.csv", row.names = F)

# 5-2 城市动销单数人数
# 动销单数(近一周)
order_alloc <- sales_detail                                                       %>% # sales_detail 为最近7日数据
  group_by(city, method)                                                          %>%
  summarise(order_quant = n_distinct(order_id))                                   %>%
  right_join(distinct(city_date_alloc[, c(1, 3)]),
             by = c("city", "method"))                                            %>%
  spread(method, order_quant)                                                     %>%
  mutate(自配单数 = ifelse(is.na(自配), 0, 自配),
         统配单数 = ifelse(is.na(统配), 0, 统配),
         自营单数 = ifelse(is.na(自营), 0, 自营))                                  %>%
  select(-自配, -统配, -自营)  

# 动销人数(近一周)
client_alloc <- sales_detail                                                      %>%
  group_by(city, method)                                                          %>%
  summarise(client_quant = n_distinct(username))                                  %>%
  right_join(distinct(city_date_alloc[, c(1, 3)]),
             by = c("city", "method"))                                            %>%
  spread(method, client_quant)                                                    %>%
  mutate(自配人数 = ifelse(is.na(自配), 0, 自配),
         统配人数 = ifelse(is.na(统配), 0, 统配),
         自营人数 = ifelse(is.na(自营), 0, 自营))                                 %>%
  select(-自配, -统配, -自营)

sales_alloc <- left_join(order_alloc, client_alloc, by = "city")

# write.csv(order_alloc, "动销单数_统配自配.csv", row.names = F)

# 5-3 城市动销
today_top_ordered <- top_ordered                                                  %>%
  filter(create_date == report_date)

sales_rate <- contract_product                                                    %>%
  left_join(product[, c("product_id", "sn")], by = "product_id")                  %>%
  left_join(admin_div, by = "area_id")                                            %>%
  select(city, district, sn)                                                      %>%
  left_join(supplier[,-2], "sn")                                                  %>%
  mutate(method = ifelse(is.na(method), "统配", method))                          %>%
  left_join(data.frame(sn = as.character(unique(today_top_ordered[,"sn"])), 
                       sold = 1, stringsAsFactors = F), "sn")                     %>%
  mutate(sold = ifelse(is.na(sold), 0, sold))                                     %>%
  group_by(city, method, sn)                                                      %>%
  summarise(sold = first(sold))                                                   %>%
  group_by(method, city)                                                          %>%
  summarise(marketable_sn_no = n(), sold_sn_no = sum(sold))                       %>%
  mutate(sales_rate = sold_sn_no / marketable_sn_no)

today_top_ordered %>% group_by(city, method) %>% summarise(count = n_distinct(sn))
# write.csv(sales_rate, "动销率.csv", row.names = F)

names(today_sold) <- c("市", "商品名称", "当日销售件数", "最近30日销售件数")
# names(order_alloc) <- c("市", "统配", "自配")
names(sales_rate) <- c("配送方式", "市", "上架商品数", "当日动销商品数", "当日动销率")

wb <- loadWorkbook("其它数据补充.xlsx", create = T)
removeSheet(wb, sheet = "others")
createSheet(wb, "others")
createSheet(wb, "近7日销售明细")
writeWorksheet(wb, as.data.frame(today_sold), sheet = "others", startCol = 1, header = T, rownames = F)
writeWorksheet(wb, as.data.frame(sales_alloc), sheet = "others", startCol = 6, header = T, rownames = F)
writeWorksheet(wb, as.data.frame(sales_rate), sheet = "others", startCol = 14, header = T, rownames = F)
writeWorksheet(wb, as.data.frame(sales_detail), sheet = "近7日销售明细", startCol = 1, header = T, rownames = F)
saveWorkbook(wb)



#------#
# 不用 #
#------#
suppressPackageStartupMessages(library(RMySQL))

conn <- dbConnect(MySQL(), dbname = "xinyunlian", username="analyst", password="123456",host="192.168.4.240",port=3306)
dbSendQuery(conn,'SET NAMES gbk')
ptm <- proc.time()
member <- dbReadTable(conn, "xinyunlian_member")
proc.time() - ptm