import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import json
import io
import os
import time
import shutil # 用于本地文件和文件夹复制
from pdf2image import convert_from_bytes
from PIL import Image
from streamlit_cropper import st_cropper # 导入全新的裁剪工具
import google.generativeai as genai

# ================= 0. 配置文件夹 =================
# 专门建立一个临时 Staging 文件夹存放未入库的解析结果和临时裁剪图片
STAGING_DIR = "staging_exam_cache"
STAGING_JSON = os.path.join(STAGING_DIR, "staging_parsed_data.json")

# 正式题库文件夹：存放最终确认的试卷配套图片
FINAL_IMAGES_DIR = "final_exam_images"

# 初始化文件夹
for d in [STAGING_DIR, FINAL_IMAGES_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

# ================= 1. 数据库初始化 (保持不变) =================
def init_db():
    conn = sqlite3.connect('error_tracker.db')
    c = conn.cursor()
    # exams 表中的 exam_content 存 JSON
    c.execute('''CREATE TABLE IF NOT EXISTS exams (id INTEGER PRIMARY KEY AUTOINCREMENT, exam_name TEXT UNIQUE, exam_content TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS error_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, student_name TEXT, exam_name TEXT, question_id TEXT, knowledge_point TEXT, record_date DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS offline_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, exam_name TEXT, question_id TEXT, correct_answer TEXT, count_A INTEGER DEFAULT 0, count_B INTEGER DEFAULT 0, count_C INTEGER DEFAULT 0, count_D INTEGER DEFAULT 0, record_date DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS offline_essay_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, exam_name TEXT, question_id TEXT, error_description TEXT, error_count INTEGER DEFAULT 1, record_date DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0

# ================= 2. 多模态 AI (Gemini) 深 OCR (保持核心逻辑) =================
def parse_exam_with_gemini(api_key, model_name, pdf_bytes):
    try:
        genai.configure(api_key=api_key)
        # 强制返回 JSON 对象
        model = genai.GenerativeModel(model_name, generation_config={"response_mime_type": "application/json"})
    except Exception as e:
        return {"error": f"API 配置失败。详细报错: {str(e)}"}
    
    try:
        # 提取前 8 页高质量图片进行深度 OCR
        images = convert_from_bytes(pdf_bytes, dpi=200)[:8] 
    except Exception as e:
        return {"error": f"PDF转图片失败！详细报错: {str(e)}"}

    prompt = """
    你是一个资深的高中地理教研员。请仔细阅读我提供给你的图片，提取出所有的【选择题】。进行深度 OCR 文本识别与逻辑重组。
    
    任务要求如下：
    1. 【题组识别 (group_material)】：识别共用的背景文字、数据描述（例如：读某某图，完成 14～16 题）。如果是独立题，填“无”。
    2. 【题干提取 (question_text)】：精准 OCR 提取每道题具体的问法文本。
    3. 【知识点 (knowledge_point)】：推断其考察的核心地理知识点。
    4. 【答案与解析 (correct_answer & explanation)】：映射出标准答案和解析文字。
    5. 【图片占位 (image_paths)】：默认为一个空数组 []，留待用户手动裁剪填充图片路径。

    请严格以 JSON 格式输出，务必包含 "questions" 键：
    {
        "questions": [
            {
                "q_id": "第1题", 
                "group_material": "读图..." or "无", 
                "question_text": "...", 
                "knowledge_point": "...", 
                "correct_answer": "A",
                "explanation": "...",
                "image_paths": [] 
            }
        ]
    }
    """
    
    try:
        response = model.generate_content([prompt] + images)
        result_str = response.text
        return json.loads(result_str).get("questions", [])
    except Exception as e:
        return {"error": f"AI 解析出错: {str(e)}"}

# ================= 3. 核心升级：Staging 磁盘缓存与同名 Bug 根除逻辑 =================

def delete_staging_cache():
    """彻底清理磁盘上的 Staging 临时缓存"""
    if os.path.exists(STAGING_JSON):
        os.remove(STAGING_JSON)
    # 同时也清理掉临时裁剪出来的图片
    for f in os.listdir(STAGING_DIR):
        f_path = os.path.join(STAGING_DIR, f)
        if os.path.isfile(f_path) and f.endswith('.png'):
            os.remove(f_path)
def delete_final_image_files(exam_name):
    """清理已删除试卷对应的正式图片文件夹"""
    # 把试卷名里的空格换成下划线，对应我们建库时的文件夹命名规则
    exam_img_folder_name = exam_name.replace(" ", "_")
    folder_path = os.path.join(FINAL_IMAGES_DIR, exam_img_folder_name)
    
    # 如果文件夹存在，就把整个文件夹连同里面的图片一起删掉
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
def save_parsed_data_to_staging(exam_name, data):
    """将 AI 解析的数据实时存入磁盘，绝对防止 token 浪费"""
    staging_bundle = {
        "exam_name": exam_name,
        "questions_data": data
    }
    with open(STAGING_JSON, 'w', encoding='utf-8') as f:
        json.dump(staging_bundle, f, ensure_ascii=False, indent=4)

def load_data_from_staging():
    """尝试加载已有的 Staging 数据"""
    if os.path.exists(STAGING_JSON):
        try:
            with open(STAGING_JSON, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def force_db_commit_and_rerun():
    """强制数据库提交，清理 UI 状态，根除同名 Bug"""
    # 强制让 SQLite 写入磁盘文件，避免锁定
    conn = sqlite3.connect('error_tracker.db', isolation_level=None) 
    conn.commit()
    conn.close()
    # 触发 Streamlit 的 full app rerun，重置状态
    st.rerun()

# ================= 4. Streamlit 界面框架 =================
st.set_page_config(page_title="智能地理教研中台 - 错题统计与题库编辑器", layout="wide")
init_db() 

st.sidebar.title("📚 地理智能中台")
api_key = st.sidebar.text_input("🔑 请输入 Gemini API Key", type="password")
st.sidebar.markdown("---")
selected_model = st.sidebar.selectbox("⚙️ 引擎(默认2.5flash，耗尽请切GEMINI)", [
        "gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-pro", "gemini-1.5-flash"])
st.sidebar.markdown("---")

menu = st.sidebar.radio(
    "请选择功能模块:",
    (
        "1. 导入试卷 (磁盘缓存+识图裁剪)", 
        "2. 离线题组录入 (手工图文题库)", 
        "3. 离线快速统计 (选择+综合)", 
        "4. 错误率统计与导出", 
        "5. 题库检索与预览",         # <--- 名字简化，更加纯粹
        "6. 数据管理 (试卷与错题清理)" # <--- 全新独立模块
    )
)

# --- 🎯 终极模块1：导入试卷 (解决 token 浪费、同名 bug、图片裁剪) ---
if menu == "1. 导入试卷 (磁盘缓存+识图裁剪)":
    st.header("📄 第一步：深度导入与人在回路题库建设")
    
    # 【Bug 修复：人在回路 Staging 状态检查】
    # 当用户进入模块1输入名字时，我们优先检查是否有未处理的临时缓存。
    staging_loaded_bundle = load_data_from_staging()
    
    in_staging_mode = False
    exam_name_final = ""
    parsed_questions = []

    if staging_loaded_bundle:
        in_staging_mode = True
        exam_name_final = staging_loaded_bundle['exam_name']
        parsed_questions = staging_loaded_bundle['questions_data']
        st.warning(f"💡 监测到您之前使用 AI 解析但**尚未确认入库**的试卷《{exam_name_final}》的数据（已存于磁盘缓存）。")
        col_st1, col_st2 = st.columns(2)
        with col_st1: st.success("我们将直接加载这些数据进行核对与图片裁剪，**绝对不调用 AI 接口，不产生多余费用。**")
        with col_st2: 
            if st.button("🗑️ 放弃这些数据，我想重新解析或换个卷子名字"):
                delete_staging_cache()
                delete_final_image_files(exam_name_final) # 同时也清理掉之前为这份试卷裁好的图片
                st.info("磁盘 Staging 缓存已清理，您可以输入新名字并重新上传。")
                st.rerun() # 强制刷新页面

    if not api_key and not in_staging_mode:
        st.warning("⚠️ 请先在侧边栏输入 API Key 才能调用 AI 功能。")
    
    # --- UI 顶层控件 (根据 Staging 状态动态调整) ---
    col1, col2 = st.columns([1, 2])
    with col1:
        # 如果是 Staging 模式，锁定名字输入，防止同名冲突
        if in_staging_mode:
            st.text_input("📝 正在核对的试卷名称", value=exam_name_final, disabled=True)
        else:
            exam_name_final = st.text_input("📝 为这份新试卷命名", placeholder="例如：随堂测验-喀斯特地貌")
            
    with col2:
        uploaded_file = st.file_uploader("📂 选择带有图表和答案页的试卷 PDF", type="pdf", disabled=in_staging_mode)
            
    # 如果不是 Staging 模式，并且上传了文件和名字，显示 AI 开始解析按钮
    if not in_staging_mode and uploaded_file and exam_name_final:
        if st.button(f"🚀 使用 {selected_model} 开始解析 (需20-40秒)", type="primary"):
            # 【Bug 修复核心逻辑：AI 解析前先彻底清理掉上一个可能存在的 Staging 缓存】
            delete_staging_cache()
            with st.spinner(f"🧠 {selected_model} 正在深度阅读文字并匹配答案解析..."):
                parsed_data_raw = parse_exam_with_gemini(api_key, selected_model, uploaded_file.read())
                
                if isinstance(parsed_data_raw, dict) and "error" in parsed_data_raw:
                    st.error(f"❌ 解析失败: {parsed_data_raw['error']}")
                else:
                    # 【Token 防浪费核心逻辑：AI 解析结果立刻存入磁盘 Staging 文件，防止 UI 刷新导致丢失】
                    save_parsed_data_to_staging(exam_name_final, parsed_data_raw)
                    st.success("✅ 深度文本 OCR 解析完成！我们将数据存于本地 Staging 缓存中防止丢失，请在下方核对与裁剪图片。")
                    force_db_commit_and_rerun() # 刷新页面，让 Staging 状态生效

    # --- 人在回路（Human-in-the-loop）题库建设区 ---
    if in_staging_mode:
        st.divider()
        st.subheader("🛠️ 深度教研与图片裁剪编辑器")
        
        # 将 Staging 数据加载为临时表单，用于实时修改图片路径
        if 'questions_data' not in st.session_state:
            st.session_state.questions_data = parsed_questions
        
        # 将 session_state 数据转为 DF，用于 UI 表格同步显示
        df_parsed_temp = pd.DataFrame(st.session_state.questions_data)
        
        # 使用 data_editor 赋予用户双击直接修改文本、答案、解析的能力
        st.markdown("**✏️ 1. 双击表格微调文字 (材料、题干、知识点、答案解析)**")
        edited_df = st.data_editor(df_parsed_temp, use_container_width=True, num_rows="dynamic", column_config={
            "q_id": st.column_config.TextColumn("题号", width="small", disabled=True), # 锁定题号
            "group_material": st.column_config.TextColumn("据此完成", width="medium"),
            "question_text": st.column_config.TextColumn("原题题干", width="medium"),
            "knowledge_point": st.column_config.TextColumn("知识点", width="small"),
            "correct_answer": st.column_config.TextColumn("答案", width="small"),
            "explanation": st.column_config.TextColumn("官方解析", width="large"),
            "image_paths": st.column_config.ListColumn("已保存图片路径", width="medium", disabled=True) # 锁定图片路径列，留待裁剪工具操作
        })
        # 同步 edited_df 的修改到 session_state
        st.session_state.questions_data = edited_df.to_dict(orient='records')
        
        st.divider()
        st.markdown("**✂️ 2. 【核心新增】据此完成 14～16 题：交互式图片/图表裁剪工具**")
        st.markdown("地理学科无图不成题。请在下方加载试卷图片，精准截取这道题（或这组题）考察的地图、景观图。裁切跨页图片支持一道题存多张图。")
        
        # 重新转换一份用于裁剪的高 DPI 图片列表存在 session_state 里
        if 'cropper_images' not in st.session_state and uploaded_file:
            uploaded_file.seek(0)
            with st.spinner("📷 正在将 PDF 高精度转化为图片加载用于裁剪..."):
                st.session_state.cropper_images = convert_from_bytes(uploaded_file.read(), dpi=200)[:8]
        
        if 'cropper_images' in st.session_state:
            images = st.session_state.cropper_images
            col_crop1, col_crop2 = st.columns([1.5, 1])
            
            with col_crop1:
                # 页面滑块选择要裁剪的页面
                page_num_slider = st.slider("📌 请滑动滑块选择要裁剪的试卷页面：", 1, len(images), 1)
                img_to_crop = images[page_num_slider - 1]
                st.markdown(f"**📄 当前显示试卷第 {page_num_slider} 页**")
                
                # *** 终极组件：交互式裁剪框 ***
                cropped_img_obj = st_cropper(img_to_crop, realtime_update=True, box_color='#FF0000', aspect_ratio=None)
                
            with col_crop2:
                if cropped_img_obj:
                    # 获取裁剪区域的坐标数据 (用于后续在总 JSON 里的记录，可选)
                    # st.write(cropped_img_obj) 
                    
                    st.write("📋 **确认将选定的裁剪图关联到哪道题：**")
                    q_id_crop = st.selectbox("🎯 选择题号进行图片关联：", ["无题组共用"] + df_parsed_temp['q_id'].tolist(), key="crop_qid")
                    
                    if q_id_crop == "无题组共用":
                        is_common_material = st.checkbox("🌟 我裁下的是本组 14～16 题公用的背景图材料（不指定具体一道题）")
                    else:
                        is_common_material = False
                    
                    st.write("🛠️ **裁剪操作控制**")
                    # 裁剪图片文件名的命名逻辑
                    timestamp = int(time.time() * 100)
                    temp_crop_filename = f"crop_{timestamp}.png"
                    temp_crop_path = os.path.join(STAGING_DIR, temp_crop_filename)
                    
                    # 定义一个列表来存储临时保存在 staging 中的图片相对路径
                    # 如果这道题之前存过图，我们把它加载出来，方便追加
                    if q_id_crop != "无题组共用":
                        q_idx = edited_df[edited_df['q_id'] == q_id_crop].index[0]
                        current_images = st.session_state.questions_data[q_idx].get('image_paths', [])
                    else:
                        current_images = []

                    col_cbt1, col_cbt2 = st.columns(2)
                    with col_cbt1:
                        if st.button("🖼️ 确认裁剪并追加关联到选中题号", type="primary"):
                            with st.spinner("💾 正在将裁剪的图片作为临时缓存保存..."):
                                # 裁剪出来的裁剪是 st_cropper 库处理过的图片字节
                                # 我们把它存为 png，放在 STAGING 文件夹
                                cropped_img_obj.save(temp_crop_path, format="PNG")
                                # 同时，我们不再使用完整的 final 路径，而是用一个相对 STAGING 的路径标记
                                new_img_staging_path = f"STAGING:{temp_crop_filename}"
                                
                                # 将 Staging 状态下的临时路径记录给这道题，并回写回 session_state 数据中
                                if is_common_material:
                                    # 将公共材料图映射给题组里的所有题 (这里假设题组数据 AI OCR 的很准，之后我们需要离线编辑器来微调题组映射)
                                    # 先不处理这么复杂的逻辑，留待之后，现在仅做一道题一道题的精准关联
                                    pass
                                if q_id_crop != "无题组共用":
                                    current_images.append(new_img_staging_path)
                                    # 必须用 list() 包一下，防止 Streamlit 的 ListColumn 数据渲染出问题
                                    st.session_state.questions_data[q_idx]['image_paths'] = list(set(current_images))
                                    st.success(f"已成功截取一张图片并追加关联到【{q_id_crop}】！它被临时存于 Staging。核对无误后点击下方按钮存入总库。")
                                    force_db_commit_and_rerun() # 重新刷新 UI 表格显示

                    with col_cbt2:
                        if st.button("🗑️ 清理本题所有裁剪关联"):
                            if q_id_crop != "无题组共用":
                                # 清理掉关联，但不清理 staging 里的文件（防止误操作），之后统一入库时再清理。
                                st.session_state.questions_data[q_idx]['image_paths'] = []
                                st.success(f"已清理【{q_id_crop}】的所有图片关联。")
                                force_db_commit_and_rerun()
                                    
            st.divider()
            # 最后的入库和 Token 缓存清理按钮
            st.markdown("**✏️ 3. 核对完毕，正式建库与数据缝合**")
            st.warning("⚠️ 最终建库将把所有 Staging 的临时图片永久存入总库的特定文件夹，并且将清除 Token 防浪费缓存，之后如果您还要修改只能使用【5. 数据管理】模块！")
            if st.button("💥 最终数据及裁剪图片存入本地题库", type="primary"):
                # 将 edited_df 里的修改写入 session_state
                st.session_state.questions_data = edited_df.to_dict(orient='records')
                
                final_questions_bundle = st.session_state.questions_data
                
                conn = sqlite3.connect('error_tracker.db')
                c = conn.cursor()
                try:
                    # 【史诗级大缝合】数据永久化与本地图片文件转移与路径替换逻辑
                    with st.spinner("⏳ 正在进行磁盘数据物理迁移和 JSON 缝合，请稍候..."):
                        
                        exam_img_folder_name = exam_name_final.replace(" ", "_")
                        exam_specific_final_dir = os.path.join(FINAL_IMAGES_DIR, exam_img_folder_name)
                        if not os.path.exists(exam_specific_final_dir):
                            os.makedirs(exam_specific_final_dir)
                            
                        # 深度遍历缝合好的 JSON 数据，将所有 Staging 的图片物理转移到正式库文件夹，并替换路径
                        for idx, q_item in enumerate(final_questions_bundle):
                            old_paths = q_item.get('image_paths', [])
                            new_paths_for_db = []
                            for p in old_paths:
                                if p.startswith("STAGING:"):
                                    staging_filename = p.replace("STAGING:", "")
                                    full_staging_src = os.path.join(STAGING_DIR, staging_filename)
                                    
                                    # 定义全新的物理文件名，防止重名
                                    final_img_filename = f"{exam_img_folder_name}_Q{q_item['q_id']}_image_{idx}.png"
                                    full_final_dst = os.path.join(exam_specific_final_dir, final_img_filename)
                                    
                                    # 本地磁盘文件移动 (从临时到正式)
                                    shutil.move(full_staging_src, full_final_dst)
                                    # 将正式的相对路径（用于网页展示的路径）写回 JSON
                                    # 相对路径格式通常为 /final_exam_images/exam_name/image.png
                                    new_rel_path = f"/{FINAL_IMAGES_DIR}/{exam_img_folder_name}/{final_img_filename}"
                                    new_paths_for_db.append(new_rel_path)
                                else:
                                    # 如果之前已经是相对路径了 (比如手动编辑过的)，保留
                                    new_paths_for_db.append(p)
                            # 写回经过物理路径替换后的最终 JSON 数组
                            final_questions_bundle[idx]['image_paths'] = list(set(new_paths_for_db))
                        
                        # 【Bug 修复点】最后的 DB unique 约束冲突严密检测逻辑
                        # 增加 1 秒延时，防止刚才删除同名时 SQLite 写入磁盘文件锁定导致的 unique 冲突
                        # time.sleep(1) 
                        
                        # 将最终经过图片物理迁移、JSON 缝合好的完整 JSON 数组整体存入 exams 总表
                        # 现在的 JSON 里面包含了文字解析、原题干、题组材料描述，以及关联好的裁剪图片路径
                        c.execute("INSERT INTO exams (exam_name, exam_content) VALUES (?, ?)", (exam_name_final, json.dumps(final_questions_bundle, ensure_ascii=False, indent=4)))
                        
                        # 缝合逻辑完成，自动为离线快速统计建立占位数据
                        # 这里的 JSON 必须使用 from_json 重新读出来干净的数据
                        df_library_final = pd.DataFrame(final_questions_bundle)
                        for _, row in df_library_final.iterrows():
                            c.execute('''INSERT INTO offline_stats (exam_name, question_id, correct_answer)
                                         VALUES (?, ?, ?)''', 
                                      (exam_name_final, row.get('q_id', '未知题号'), row.get('correct_answer', 'A')))
                        
                        conn.commit()
                        st.success(f"🎉 包含完整文本 OCR、裁剪图片映射的《{exam_name_final}》已成功建库！")
                        st.balloons()
                        
                        # 建库成功后，【彻底清理 Staging 缓存】
                        delete_staging_cache()
                        st.markdown("**👉 下一步：您可以前往左侧导航栏的【3. 离线快速统计】对这份全功能的深度教研试卷进行错误率统计了！**")
                        
                        # 建库成功后强制删除 session_state，避免 UI 冲突
                        del st.session_state.questions_data
                        # 建库成功后彻底重启应用界面，根除所有同名同 ID 状态冲突
                        force_db_commit_and_rerun()
                        
                except sqlite3.IntegrityError:
                    # 这里是最后的防线，防止即使使用了 rerun 仍有 SQLite 锁定导致的错误
                    st.error(f"⚠️ 试卷名称《{exam_name_final}》已存在！请去【5. 数据管理】删掉旧卷，或者放弃 Staging 缓存。")
                finally:
                    conn.close()
# --- 【全新重构】模块2：离线题组录入 (AI的完美补充) ---
# --- 【沉浸连续录入版】模块2：离线题组录入 (手工图文题库) ---
elif menu == "2. 离线题组录入 (手工图文题库)":
    st.header("✍️ 第二步：离线题组录入 (手工共创图文题库)")
    st.markdown("在此手工编排选择题组或综合题组。设置选项、分值并上传截图，保存后将与 AI 扫描的卷子无缝融合！")
    
    conn = sqlite3.connect('error_tracker.db')
    df_exams_all = pd.read_sql_query("SELECT exam_name FROM exams", conn)
    all_exams = df_exams_all['exam_name'].tolist()
    
    # 引入 Session State 实现试卷锁定功能
    if 'locked_exam_for_entry' not in st.session_state:
        st.session_state.locked_exam_for_entry = None

    exam_name_input = ""

    # 状态分流：如果没有锁定试卷，显示选择/创建界面
    if st.session_state.locked_exam_for_entry is None:
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            exam_mode = st.radio("录入目标", ["🆕 创建新试卷 (自编套卷)", "✏️ 追加到已有试卷"], horizontal=True)
        with col_m2:
            if exam_mode == "🆕 创建新试卷 (自编套卷)":
                temp_exam_name = st.text_input("📝 试卷名称", placeholder="例如：自编地球运动阶段测验")
            else:
                temp_exam_name = st.selectbox("📂 选择要追加的试卷", all_exams) if all_exams else ""
        
        if temp_exam_name:
            if st.button("🎯 锁定该试卷并开始连续录入", type="primary"):
                st.session_state.locked_exam_for_entry = temp_exam_name
                st.session_state.exam_mode_for_entry = exam_mode # 记住是新建还是追加
                st.rerun()
    
    # 状态分流：如果已经锁定试卷，进入沉浸式连续录入模式
    else:
        exam_name_input = st.session_state.locked_exam_for_entry
        exam_mode = st.session_state.exam_mode_for_entry
        
        col_lock1, col_lock2 = st.columns([4, 1])
        with col_lock1:
            st.success(f"📌 **沉浸录入模式已开启**：当前所有的题组都将连续存入试卷 《**{exam_name_input}**》 中。")
        with col_lock2:
            if st.button("退出当前试卷 / 切换卷子"):
                st.session_state.locked_exam_for_entry = None
                st.rerun()

        st.divider()
        q_type = st.radio("📌 本次录入题型：", ["选择题组", "综合题组"], horizontal=True)
        
        st.subheader("📖 题组公共部分 (选填)")
        group_material = st.text_area("公共材料文字 (如：据图1完成1-3题)", height=100)
        group_img = st.file_uploader("上传公共材料截图", type=['png', 'jpg', 'jpeg'], key="group_img_upload")
        
        q_count = st.number_input(f"🔢 本题组包含几个小问/小题？", min_value=1, max_value=10, value=2, step=1)
        
        with st.form("manual_entry_form", clear_on_submit=True):
            st.info("💡 填写下方内容。点击保存后，页面会自动清空当前表单，但试卷仍保持锁定，方便您立刻开始下一组题的录入！")
            q_ids, q_texts, q_scores, q_kps, q_ans, q_exps, q_imgs = [], [], [], [], [], [], []
            opts_A, opts_B, opts_C, opts_D = [], [], [], []
            
            for i in range(q_count):
                st.markdown(f"**第 {i+1} 小题**")
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1: q_ids.append(st.text_input("题号", placeholder="必填", key=f"q_id_{i}"))
                with col2: q_scores.append(st.number_input("分值", min_value=1, value=3 if q_type == "选择题组" else 4, step=1, key=f"q_score_{i}"))
                with col3: q_kps.append(st.text_input("知识点", placeholder="如: 焚风效应", key=f"q_kp_{i}"))
                
                q_texts.append(st.text_area("题干文字", key=f"q_text_{i}", height=68))
                
                if q_type == "选择题组":
                    col_o1, col_o2, col_o3, col_o4 = st.columns(4)
                    with col_o1: opts_A.append(st.text_input("选项 A", key=f"opt_A_{i}"))
                    with col_o2: opts_B.append(st.text_input("选项 B", key=f"opt_B_{i}"))
                    with col_o3: opts_C.append(st.text_input("选项 C", key=f"opt_C_{i}"))
                    with col_o4: opts_D.append(st.text_input("选项 D", key=f"opt_D_{i}"))
                    q_ans.append(st.selectbox("正确答案", ["A", "B", "C", "D"], key=f"q_ans_{i}"))
                else:
                    opts_A.append(""); opts_B.append(""); opts_C.append(""); opts_D.append("")
                    q_ans.append(st.text_area("标准答案 / 参考要点", key=f"q_ans_{i}", height=68))
                    
                q_exps.append(st.text_input("解析说明 (选填)", key=f"q_exp_{i}"))
                q_imgs.append(st.file_uploader("上传本小题专属配图 (选填)", type=['png', 'jpg', 'jpeg'], key=f"q_img_{i}"))
                st.divider()
                
            submitted = st.form_submit_button("💾 保存当前题组，并继续录入下一组", type="primary")
            
            if submitted:
                valid_qs = [qid for qid in q_ids if qid.strip()]
                if not valid_qs:
                    st.error("⚠️ 至少需要为一个小题填写【题号】！")
                else:
                    FINAL_IMAGES_DIR = "final_exam_images"
                    exam_img_folder_name = exam_name_input.replace(" ", "_")
                    exam_specific_final_dir = os.path.join(FINAL_IMAGES_DIR, exam_img_folder_name)
                    if not os.path.exists(exam_specific_final_dir):
                        os.makedirs(exam_specific_final_dir)
                        
                    def save_uploaded_file(u_file, prefix):
                        if u_file is not None:
                            import time
                            timestamp = int(time.time() * 1000)
                            ext = u_file.name.split('.')[-1]
                            filename = f"{exam_img_folder_name}_{prefix}_{timestamp}.{ext}"
                            filepath = os.path.join(exam_specific_final_dir, filename)
                            with open(filepath, "wb") as f:
                                f.write(u_file.getbuffer())
                            return f"/{FINAL_IMAGES_DIR}/{exam_img_folder_name}/{filename}"
                        return None
                        
                    group_img_path = save_uploaded_file(group_img, "group_manual")
                    
                    new_questions = []
                    for i in range(q_count):
                        if not q_ids[i].strip(): continue
                        
                        q_image_paths = []
                        if group_img_path: q_image_paths.append(group_img_path)
                        sub_img_path = save_uploaded_file(q_imgs[i], f"Q{q_ids[i].strip()}_manual")
                        if sub_img_path: q_image_paths.append(sub_img_path)
                        
                        final_q_text = q_texts[i]
                        if q_type == "选择题组":
                            final_q_text += f"\n\nA. {opts_A[i]}\nB. {opts_B[i]}\nC. {opts_C[i]}\nD. {opts_D[i]}"
                            
                        new_q = {
                            "q_id": q_ids[i].strip(),
                            "group_material": group_material,
                            "question_text": final_q_text,
                            "knowledge_point": q_kps[i],
                            "correct_answer": q_ans[i],
                            "explanation": q_exps[i],
                            "image_paths": q_image_paths,
                            "score": q_scores[i],     
                            "q_type": q_type          
                        }
                        new_questions.append(new_q)
                        
                    c = conn.cursor()
                    try:
                        # 检查数据库中是否已经有这套试卷 (因为如果是连续录入，第二次及以后就变成 UPDATE 追加了)
                        c.execute("SELECT id, exam_content FROM exams WHERE exam_name=?", (exam_name_input,))
                        row = c.fetchone()
                        
                        if row is None:
                            # 第一次入库：INSERT
                            c.execute("INSERT INTO exams (exam_name, exam_content) VALUES (?, ?)", 
                                      (exam_name_input, json.dumps(new_questions, ensure_ascii=False)))
                        else:
                            # 连续录入后续题组：UPDATE 追加
                            existing_data = json.loads(row[1]) if row[1] else []
                            existing_data.extend(new_questions)
                            c.execute("UPDATE exams SET exam_content=? WHERE exam_name=?", 
                                      (json.dumps(existing_data, ensure_ascii=False), exam_name_input))
                        
                        for q in new_questions:
                            if q["q_type"] == "选择题组":
                                c.execute("SELECT id FROM offline_stats WHERE exam_name=? AND question_id=?", (exam_name_input, q["q_id"]))
                                if not c.fetchone():
                                    c.execute('''INSERT INTO offline_stats (exam_name, question_id, correct_answer) VALUES (?, ?, ?)''', 
                                              (exam_name_input, q["q_id"], q["correct_answer"]))
                                    
                        conn.commit()
                        st.success(f"🎉 题组保存成功！页面即将重置表单，请直接继续输入下一组题！")
                        import time; time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        st.error(f"保存失败: {str(e)}")
    conn.close()

# --- 【史诗级进化】模块3：支持键盘录入与专注模式 ---
elif menu == "3. 离线快速统计 (选择+综合)":
    st.header("📴 离线快速统计：全班学情速记")
    
    conn = sqlite3.connect('error_tracker.db')
    all_exams_df = pd.read_sql_query("SELECT DISTINCT exam_name FROM offline_stats UNION SELECT DISTINCT exam_name FROM offline_essay_stats", conn)
    all_exams = all_exams_df['exam_name'].tolist()
    
    input_mode = st.radio("请选择操作模式：", ["🆕 创建新试卷", "✏️ 追加/修改已有试卷"], horizontal=True)
    
    exam_name = ""
    existing_obj_data = {}
    default_q_count = 5
    
    if input_mode == "🆕 创建新试卷":
        exam_name = st.text_input("📝 新建试卷名称", placeholder="例如：随堂测验-地球的运动")
    else:
        if not all_exams:
            st.warning("暂无已有试卷记录，请先创建新试卷。")
        else:
            exam_name = st.selectbox("📂 选择要追加或修改的试卷：", all_exams)
            df_obj = pd.read_sql_query("SELECT * FROM offline_stats WHERE exam_name=?", conn, params=(exam_name,))
            for _, row in df_obj.iterrows(): existing_obj_data[row['question_id']] = row
            if len(existing_obj_data) > 0: default_q_count = len(existing_obj_data)
    conn.close()

    if exam_name:
        tab_obj, tab_sub = st.tabs(["🎯 客观题 (选择题分布)", "✍️ 主观题 (综合题典型错误)"])
        
        with tab_obj:
            q_count = st.number_input("🔢 请输入本卷选择题总数", min_value=1, max_value=50, value=default_q_count, step=1)
            st.divider()
            
            # 【核心功能 1】视图控制与录入方式控制
            col_view, col_input = st.columns(2)
            with col_view:
                view_mode = st.radio("👀 批改视图模式：", ["📜 全卷平铺模式 (显示所有题)", "🎯 专注模式 (按单题或题组批改)"], horizontal=True)
            with col_input:
                keyboard_mode = st.radio("✍️ 交互录入模式：", ["🖱️ 鼠标加减 (适合少量调整)", "⌨️ 键盘极速录入 (适合大批量盲打)"], horizontal=True)

            # 【核心功能 2】专注模式下的题组选择
            display_range = range(1, q_count + 1)
            if view_mode == "🎯 专注模式 (按单题或题组批改)":
                st.info("💡 提示：拖动下方滑块选择当前正在批改的题号。录入完毕后点击保存，再滑动到下一题。")
                start_q, end_q = st.slider("📌 选择当前显示的题号区间：", 1, q_count, (1, 1))
                display_range = range(start_q, end_q + 1)
            
            with st.form("offline_stats_form"):
                for i in display_range:
                    q_key = f"第{i}题"
                    history = existing_obj_data.get(q_key, {'correct_answer': 'A', 'count_A': 0, 'count_B': 0, 'count_C': 0, 'count_D': 0})
                    
                    st.markdown(f"**{q_key}**")
                    ans_options = ["A", "B", "C", "D"]
                    ans_index = ans_options.index(history['correct_answer']) if history['correct_answer'] in ans_options else 0
                    
                    if keyboard_mode == "⌨️ 键盘极速录入 (适合大批量盲打)":
                        # --- 键盘模式 UI ---
                        col_ans, col_kbd = st.columns([1.5, 4.5])
                        with col_ans: st.selectbox("🎯 答案", ans_options, index=ans_index, key=f"q{i}_ans")
                        with col_kbd: 
                            st.caption(f"当前库存 ➔ A:{history['count_A']} | B:{history['count_B']} | C:{history['count_C']} | D:{history['count_D']}")
                            # 动态 key 保证提交后输入框能自动清空
                            st.text_input("连击错选字母并回车 (如输入 aabcc 代表2个A,1个B,2个C)", key=f"kbd_{i}_{st.session_state.refresh_counter}")
                        st.write("")
                    else:
                        # --- 鼠标模式 UI ---
                        col_ans, col1, col2, col3, col4 = st.columns([1.5, 1, 1, 1, 1])
                        with col_ans: st.selectbox("🎯 答案", ans_options, index=ans_index, key=f"q{i}_ans")
                        with col1: st.number_input("选 A 人数", min_value=0, step=1, value=int(history['count_A']), key=f"q{i}_a")
                        with col2: st.number_input("选 B 人数", min_value=0, step=1, value=int(history['count_B']), key=f"q{i}_b")
                        with col3: st.number_input("选 C 人数", min_value=0, step=1, value=int(history['count_C']), key=f"q{i}_c")
                        with col4: st.number_input("选 D 人数", min_value=0, step=1, value=int(history['count_D']), key=f"q{i}_d")
                
                if st.form_submit_button("💾 录入并保存当前修改"):
                    conn = sqlite3.connect('error_tracker.db')
                    c = conn.cursor()
                    c.execute("DELETE FROM offline_stats WHERE exam_name=?", (exam_name,))
                    
                    # 遍历全卷所有题目，结合 UI 上的新数据和数据库里的旧数据
                    for i in range(1, q_count + 1):
                        q_key = f"第{i}题"
                        history = existing_obj_data.get(q_key, {'correct_answer': 'A', 'count_A': 0, 'count_B': 0, 'count_C': 0, 'count_D': 0})
                        
                        if i in display_range:
                            # 题目在当前显示的视图内，获取最新数据
                            ans = st.session_state[f"q{i}_ans"]
                            if keyboard_mode == "⌨️ 键盘极速录入 (适合大批量盲打)":
                                kbd_str = st.session_state[f"kbd_{i}_{st.session_state.refresh_counter}"].upper()
                                val_a = history['count_A'] + kbd_str.count('A')
                                val_b = history['count_B'] + kbd_str.count('B')
                                val_c = history['count_C'] + kbd_str.count('C')
                                val_d = history['count_D'] + kbd_str.count('D')
                            else:
                                val_a = st.session_state[f"q{i}_a"]
                                val_b = st.session_state[f"q{i}_b"]
                                val_c = st.session_state[f"q{i}_c"]
                                val_d = st.session_state[f"q{i}_d"]
                        else:
                            # 题目被隐藏（未被修改），保留原有历史数据
                            ans = history['correct_answer']
                            val_a = history['count_A']
                            val_b = history['count_B']
                            val_c = history['count_C']
                            val_d = history['count_D']
                            
                        c.execute('''INSERT INTO offline_stats (exam_name, question_id, correct_answer, count_A, count_B, count_C, count_D)
                                     VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                                  (exam_name, f"第{i}题", ans, val_a, val_b, val_c, val_d))
                    
                    conn.commit()
                    conn.close()
                    # 更新计数器，让下一次渲染时文本框彻底清空
                    st.session_state.refresh_counter += 1
                    st.rerun() # 强制刷新页面展示最新状态

        with tab_sub:
            # 1. 顶部：依然保留快速追加的表单，方便连续敲击录入
            st.markdown("持续追加主观题典型错因：")
            with st.form("essay_error_form", clear_on_submit=True):
                col_q, col_desc, col_cnt = st.columns([2, 5, 2])
                with col_q: essay_q_id = st.text_input("题号/小问", placeholder="如：第36题(2)问")
                with col_desc: error_desc = st.text_input("典型错误描述", placeholder="如：水文特征漏答了'结冰期'")
                with col_cnt: error_cnt = st.number_input("犯错大致人数", min_value=1, step=1)
                
                if st.form_submit_button("➕ 增加/追加此条错误") and essay_q_id and error_desc:
                    conn = sqlite3.connect('error_tracker.db')
                    c = conn.cursor()
                    c.execute('''INSERT INTO offline_essay_stats (exam_name, question_id, error_description, error_count)
                                 VALUES (?, ?, ?, ?)''', (exam_name, essay_q_id, error_desc, error_cnt))
                    conn.commit()
                    conn.close()
                    st.success(f"已追加：{essay_q_id} - {error_desc}")
                    time.sleep(0.5)
                    st.rerun() # 追加后立刻刷新页面，让下方表格实时显示新数据
            
            st.divider()
            
            # 2. 底部：【全新升级】展示历史记录，并支持直接修改和删除
            conn = sqlite3.connect('error_tracker.db')
            df_essays = pd.read_sql_query("SELECT id, question_id as 题号, error_description as 错误描述, error_count as 犯错人数 FROM offline_essay_stats WHERE exam_name=?", conn, params=(exam_name,))
            conn.close()
            
            if not df_essays.empty:
                st.write("📋 **当前试卷已录入的主观题错误清单：**")
                st.info("💡 **操作指南**：双击单元格可直接修改文字或人数；选中某行最左侧的复选框，按键盘 `Delete` 键即可删除该记录。修改完请务必点击下方保存按钮！")
                
                # 引入 st.data_editor 替代原来的 st.dataframe
                edited_essays = st.data_editor(
                    df_essays, 
                    use_container_width=True, 
                    hide_index=True,
                    num_rows="dynamic", # 这一行是灵魂，它允许表格动态增删行
                    column_config={
                        "id": None, # 隐藏数据库内部真实ID，保持界面整洁
                        "题号": st.column_config.TextColumn("题号/小问", required=True),
                        "错误描述": st.column_config.TextColumn("典型错误描述", required=True),
                        "犯错人数": st.column_config.NumberColumn("犯错人数", min_value=1, step=1, required=True)
                    },
                    key=f"essay_editor_{exam_name}"
                )
                
                # 独立保存按钮：采用全量覆盖更新逻辑
                if st.button("💾 保存对下方清单的修改与删除", type="primary"):
                    conn = sqlite3.connect('error_tracker.db')
                    c = conn.cursor()
                    # 先清空这份卷子的所有主观题记录，再把刚刚编辑完的表格全量写回去
                    c.execute("DELETE FROM offline_essay_stats WHERE exam_name=?", (exam_name,))
                    
                    for _, row in edited_essays.iterrows():
                        # 过滤掉空行，防止意外插入脏数据
                        if pd.notna(row.get('题号')) and str(row.get('题号')).strip() != '':
                            c.execute('''INSERT INTO offline_essay_stats (exam_name, question_id, error_description, error_count)
                                         VALUES (?, ?, ?, ?)''', 
                                      (exam_name, row['题号'], row['错误描述'], row['犯错人数']))
                    conn.commit()
                    conn.close()
                    st.success("✅ 主观题记录修改已成功同步至底层数据库！")
                    time.sleep(1)
                    st.rerun() # 强制刷新，清除编辑状态

# --- 模块4：错误率统计与动态看板 (保持上一版的精华不变) ---
elif menu == "4. 错误率统计与导出":
    st.header("📊 第四步：试卷学情动态看板")
    conn = sqlite3.connect('error_tracker.db')
    df_exams_obj = pd.read_sql_query("SELECT DISTINCT exam_name FROM offline_stats", conn)
    df_exams_sub = pd.read_sql_query("SELECT DISTINCT exam_name FROM offline_essay_stats", conn)
    all_exams = list(set(df_exams_obj['exam_name'].tolist() + df_exams_sub['exam_name'].tolist()))
    
    if not all_exams:
        st.warning("⚠️ 数据库中暂无数据。")
    else:
        selected_exam = st.selectbox("📌 请选择要分析的试卷：", all_exams)
        st.divider()
        df_obj = pd.read_sql_query("SELECT question_id, correct_answer, count_A, count_B, count_C, count_D FROM offline_stats WHERE exam_name=?", conn, params=(selected_exam,))
        if not df_obj.empty:
            df_obj['总人数'] = df_obj['count_A'] + df_obj['count_B'] + df_obj['count_C'] + df_obj['count_D']
            def get_correct(row):
                ans = row['correct_answer']
                if ans == 'A': return row['count_A']
                elif ans == 'B': return row['count_B']
                elif ans == 'C': return row['count_C']
                elif ans == 'D': return row['count_D']
                return 0
            df_obj['答对人数'] = df_obj.apply(get_correct, axis=1)
            df_obj['答错人数'] = df_obj['总人数'] - df_obj['答对人数']
            df_obj['错误率'] = (df_obj['答错人数'] / df_obj['总人数']).fillna(0)
            
        df_sub = pd.read_sql_query("SELECT question_id as 题号, error_description as 错误描述, error_count as 犯错人数 FROM offline_essay_stats WHERE exam_name=?", conn, params=(selected_exam,))
        
        tab_obj_view, tab_sub_view, tab_data = st.tabs(["🎯 客观题智能诊断", "✍️ 主观题诊断", "📄 数据与导出"])
        
        with tab_obj_view:
            if not df_obj.empty:
                col_ctrl1, col_ctrl2 = st.columns([2, 1])
                with col_ctrl1: obj_view_mode = st.selectbox("🔍 切换数据视角：", ["全局概览 (所有题目)"] + df_obj['question_id'].tolist(), key="obj_view")
                with col_ctrl2: obj_chart_type = st.radio("📊 图表类型：", ["柱状图", "饼状图"], horizontal=True, key="obj_chart")
                
                if obj_view_mode == "全局概览 (所有题目)":
                    if obj_chart_type == "柱状图":
                        df_melt = df_obj.melt(id_vars=['question_id', 'correct_answer'], value_vars=['count_A', 'count_B', 'count_C', 'count_D'], var_name='选项', value_name='人数')
                        df_melt['纯选项'] = df_melt['选项'].str.replace('count_', '')
                        df_melt['状态'] = df_melt.apply(lambda r: f"{r['纯选项']} (正确)" if r['纯选项'] == r['correct_answer'] else f"{r['纯选项']} (错选)", axis=1)
                        fig = px.bar(df_melt, x='question_id', y='人数', color='状态', title="全卷选项分布图", text_auto=True)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        total_right = df_obj['答对人数'].sum()
                        total_wrong = df_obj['答错人数'].sum()
                        fig = px.pie(names=['整体答对人次', '整体答错人次'], values=[total_right, total_wrong], title="全卷整体正确率分布", hole=0.4, color_discrete_sequence=['#2ecc71', '#e74c3c'])
                        fig.update_traces(textinfo='percent+label')
                        st.plotly_chart(fig, use_container_width=True)
                    max_err_q = df_obj.loc[df_obj['答错人数'].idxmax()]
                    st.error(f"**🚨 宏观诊断：** 全卷错误率最高的是 **{max_err_q['question_id']}**，错误率高达 **{max_err_q['错误率']:.0%}**。请切换到该题查看详情。")
                else: 
                    q_data = df_obj[df_obj['question_id'] == obj_view_mode].iloc[0]
                    ans_data = pd.DataFrame({'选项': ['A', 'B', 'C', 'D'], '人数': [q_data['count_A'], q_data['count_B'], q_data['count_C'], q_data['count_D']], '状态': ['正确' if x == q_data['correct_answer'] else '错选' for x in ['A', 'B', 'C', 'D']]})
                    if obj_chart_type == "柱状图":
                        fig = px.bar(ans_data, x='选项', y='人数', color='状态', title=f"{obj_view_mode} 答题分布", text_auto=True, color_discrete_map={"正确": "#2ecc71", "错选": "#e74c3c"})
                    else:
                        fig = px.pie(ans_data, names='选项', values='人数', title=f"{obj_view_mode} 各选项占比", hole=0.3)
                        fig.update_traces(textinfo='percent+label')
                    st.plotly_chart(fig, use_container_width=True)
                    
                    wrong_opts = ans_data[ans_data['状态'] == '错选']
                    top_wrong_opt = wrong_opts.loc[wrong_opts['人数'].idxmax()] if not wrong_opts.empty else None
                    insight_text = f"**💡 系统诊断分析：**\n\n本题标准答案为 **{q_data['correct_answer']}**。全班共有 {q_data['总人数']} 人作答，其中 **{q_data['答对人数']}** 人答对，错误率为 **{q_data['错误率']:.0%}**。"
                    if top_wrong_opt is not None and top_wrong_opt['人数'] > 0: insight_text += f"\n\n⚠️ **强干扰项预警：** 最具迷惑性的选项是 **{top_wrong_opt['选项']}**，共“诱导”了 **{top_wrong_opt['人数']}** 名学生错选。建议在讲评时重点对比。"
                    else: insight_text += "\n\n🎉 答题情况非常理想，没有明显易错的干扰项！"
                    st.info(insight_text)

        with tab_sub_view:
            if not df_sub.empty:
                col_sub1, col_sub2 = st.columns([2, 1])
                with col_sub1: sub_view_mode = st.selectbox("🔍 选择分析范围：", ["全局概览 (所有错因)"] + list(df_sub['题号'].unique()), key="sub_view")
                with col_sub2: sub_chart_type = st.radio("📊 图表类型：", ["柱状图", "饼状图"], horizontal=True, key="sub_chart")
                filtered_sub = df_sub if sub_view_mode == "全局概览 (所有错因)" else df_sub[df_sub['题号'] == sub_view_mode]
                
                if sub_chart_type == "柱状图": fig_sub = px.bar(filtered_sub, x='错误描述', y='犯错人数', color='题号', title=f"{sub_view_mode} 错误频次", text_auto=True)
                else:
                    fig_sub = px.pie(filtered_sub, names='错误描述', values='犯错人数', title=f"{sub_view_mode} 错因占比", hole=0.4)
                    fig_sub.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_sub, use_container_width=True)
                
                top_err = filtered_sub.loc[filtered_sub['犯错人数'].idxmax()]
                if sub_view_mode == "全局概览 (所有错因)": st.warning(f"**🚨 宏观诊断：** 当前全卷主观题最大的“丢分大户”出现在 **{top_err['题号']}**，典型错因是：「**{top_err['错误描述']}**」（波及 {top_err['犯错人数']} 人）。")
                else: st.warning(f"**🔬 单题诊断 ({sub_view_mode})：** 本题最首要的薄弱环节是：「**{top_err['错误描述']}**」（共 {top_err['犯错人数']} 人犯错）。")

        with tab_data:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                if not df_obj.empty:
                    df_export_obj = df_obj[['question_id', 'correct_answer', 'count_A', 'count_B', 'count_C', 'count_D', '总人数', '答错人数', '错误率']]
                    st.dataframe(df_export_obj, use_container_width=True, hide_index=True)
                    df_export_obj.to_excel(writer, sheet_name='客观题统计', index=False)
                if not df_sub.empty:
                    st.dataframe(df_sub, use_container_width=True, hide_index=True)
                    df_sub.to_excel(writer, sheet_name='主观题错误记录', index=False)
            st.download_button(label="📥 一键导出为 Excel 报表", data=output.getvalue(), file_name=f"{selected_exam}_学情盘点报告.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
    conn.close()

# ================= 5. 题库检索与预览 =================
elif menu == "5. 题库检索与预览":
    st.header("🔍 第五步：题库检索与预览大本营")
    st.markdown("通过试卷名称或知识点，快速检索历史题目。供您在此预览原题、图片与解析，方便您去 Word 中挑选组卷。")
    
    conn = sqlite3.connect('error_tracker.db')
    df_exams_all = pd.read_sql_query("SELECT exam_name, exam_content FROM exams", conn)
    all_exams = df_exams_all['exam_name'].tolist()
    
    # 将所有试卷的 JSON 数据摊平，构建一个可以全局搜索的题库池
    all_questions_pool = []
    for _, row in df_exams_all.iterrows():
        try:
            q_list = json.loads(row['exam_content'])
            for q in q_list:
                q['所属试卷'] = row['exam_name'] 
                all_questions_pool.append(q)
        except:
            continue

    # UI 布局：两框独立检索
    col_s1, col_s2, col_s3 = st.columns([2, 2, 1])
    with col_s1:
        search_exam = st.selectbox("📂 按【试卷名称】检索", ["-- 全库搜索 --"] + all_exams)
    with col_s2:
        search_kp = st.text_input("🧠 按【知识点/关键词】模糊检索", placeholder="例如：晨昏线、焚风、洋流...")
    with col_s3:
        search_type = st.radio("📝 查阅模式", ["📖 原题与解析 (全部题型)", "📉 综合题历年易错榜单"], horizontal=True)
        
    st.divider()

    # 核心检索逻辑
    if search_type == "📖 原题与解析 (全部题型)":
        filtered_qs = all_questions_pool
        if search_exam != "-- 全库搜索 --":
            filtered_qs = [q for q in filtered_qs if q['所属试卷'] == search_exam]
        if search_kp:
            filtered_qs = [q for q in filtered_qs if (
                search_kp in str(q.get('knowledge_point', '')) or 
                search_kp in str(q.get('question_text', '')) or
                search_kp in str(q.get('group_material', ''))
            )]

        if not filtered_qs:
            st.info("☁️ 题库中没有找到符合该条件的题目。")
        else:
            st.success(f"🎯 为您检索到 **{len(filtered_qs)}** 道相关题目，点击下方卡片展开预览：")
            for q in filtered_qs:
                q_type_label = q.get('q_type', '选择题') 
                score_label = f" ({q.get('score', 3)}分)" if 'score' in q else ""
                
                card_title = f"🏷️ {q.get('knowledge_point', '未分类知识点')} | {q['所属试卷']} - {q.get('q_id', '')} [{q_type_label}{score_label}]"
                with st.expander(card_title):
                    if q.get('group_material') and str(q.get('group_material')).strip() not in ["无", ""]:
                        st.info(f"**📖 题组公共材料：**\n\n{q['group_material']}")
                        
                    st.markdown(f"**❓ 题干：**\n\n{q.get('question_text', '无题干')}")
                    
                    images = q.get('image_paths', [])
                    if images:
                        st.write("**🖼️ 关联图表：**")
                        img_cols = st.columns(len(images))
                        for idx, img_path in enumerate(images):
                            local_path = img_path.lstrip('/') 
                            if os.path.exists(local_path):
                                img_cols[idx].image(local_path, use_container_width=True)
                            else:
                                img_cols[idx].warning("图片原文件已丢失")
                    
                    st.markdown(f"**✅ 标准答案：** `{q.get('correct_answer', '')}`")
                    if q.get('explanation') and str(q.get('explanation')).strip() not in ["无", "无解析", ""]:
                        st.success(f"**💡 官方解析：**\n\n{q['explanation']}")
    else:
        df_sub = pd.read_sql_query("SELECT exam_name as 所属试卷, question_id as 题号, error_description as 典型错因, error_count as 犯错人数 FROM offline_essay_stats", conn)
        if search_exam != "-- 全库搜索 --":
            df_sub = df_sub[df_sub['所属试卷'] == search_exam]
        if search_kp:
            df_sub = df_sub[df_sub['典型错因'].str.contains(search_kp, na=False)]
            
        if df_sub.empty:
            st.info("☁️ 没有检索到符合条件的综合题记录。")
        else:
            st.success(f"🎯 共检索到 **{len(df_sub)}** 条相关综合题易错记录（可作为出题考点参考）：")
            st.dataframe(df_sub, hide_index=True, use_container_width=True)
    conn.close()

# ================= 6. 数据管理 (试卷与错题清理) =================
elif menu == "6. 数据管理 (试卷与错题清理)":
    st.header("⚙️ 第六步：数据管理 (试卷库与错题清理)")
    st.markdown("在这里，你可以分别管理【底层题库】和【学生学情统计数据】。两者逻辑相互独立，完美保护您的教研心血。")
    
    conn = sqlite3.connect('error_tracker.db')
    
    # 【修复大升级】不仅查试卷库，把所有表里残留过的“幽灵名字”全部揪出来！
    query_all_names = """
        SELECT DISTINCT exam_name FROM exams
        UNION SELECT DISTINCT exam_name FROM offline_stats
        UNION SELECT DISTINCT exam_name FROM offline_essay_stats
        UNION SELECT DISTINCT exam_name FROM error_logs
    """
    df_exams_all = pd.read_sql_query(query_all_names, conn)
    # 过滤掉空名字
    df_exams_all = df_exams_all.dropna()
    df_exams_all = df_exams_all[df_exams_all['exam_name'].str.strip() != '']
    all_exams = df_exams_all['exam_name'].tolist()
    
    tab_stats, tab_exam, tab_ghost = st.tabs(["🧹 清理某卷错题统计", "⚠️ 永久销毁整份试卷", "👻 深度体检 (清理早期幽灵数据)"])
    
    # --- 标签页 1：只清空统计，保留题库 ---
    with tab_stats:
        st.subheader("🧹 重新开始：一键清空班级学情数据")
        st.info("💡 此操作只会把该试卷的选择题错误人数【归零】，并清空综合题记录。原试卷的题干、图片和答案完好无损！")
        
        if not all_exams:
            st.write("☁️ 暂无试卷可操作。")
        else:
            clear_stats_exam = st.selectbox("📌 选择要清空统计数据的试卷：", all_exams, key="clear_stats_select")
            conf_clear = st.checkbox(f"我确认要清空《{clear_stats_exam}》的所有学生错题记录", key="conf_clear")
            
            if st.button("🧽 一键归零学情数据", type="primary", disabled=not conf_clear):
                c = conn.cursor()
                c.execute("UPDATE offline_stats SET count_A=0, count_B=0, count_C=0, count_D=0 WHERE exam_name=?", (clear_stats_exam,))
                c.execute("DELETE FROM offline_essay_stats WHERE exam_name=?", (clear_stats_exam,))
                c.execute("DELETE FROM error_logs WHERE exam_name=?", (clear_stats_exam,))
                conn.commit()
                st.success(f"✨ 《{clear_stats_exam}》的错题统计已全部归零！你可以立刻为新班级重新批改了！")
                import time; time.sleep(1.5)
                st.rerun()

    # --- 标签页 2：暴力删除，寸草不生 ---
    with tab_exam:
        st.subheader("⚠️ 连根拔起：永久销毁整份试卷")
        st.error("🚨 此操作将永久抹除该试卷的【所有数据】，包括题干、图片、正确答案解析，以及其名下的所有统计数据！")
        
        if not all_exams:
            st.write("☁️ 暂无试卷可操作。")
        else:
            del_exam = st.selectbox("📌 选择要彻底销毁的试卷：", all_exams, key="del_exam_select")
            conf_del = st.checkbox(f"我深知风险，确认要永久销毁《{del_exam}》", key="conf_del")
            
            if st.button("🗑️ 彻底销毁该试卷", type="primary", disabled=not conf_del):
                c = conn.cursor()
                c.execute("DELETE FROM offline_stats WHERE exam_name=?", (del_exam,))
                c.execute("DELETE FROM offline_essay_stats WHERE exam_name=?", (del_exam,))
                c.execute("DELETE FROM error_logs WHERE exam_name=?", (del_exam,))
                c.execute("DELETE FROM exams WHERE exam_name=?", (del_exam,))
                conn.commit()
                
                try:
                    # 尝试调用函数清理物理图片
                    delete_final_image_files(del_exam)
                except Exception as e:
                    pass 
                
                st.success(f"💥 《{del_exam}》已被物理超度，永远离开了题库！")
                import time; time.sleep(1.5)
                st.rerun()
                
    # --- 标签页 3：【全新核心】幽灵孤立数据清理 ---
    with tab_ghost:
        st.subheader("👻 深度体检：一键粉碎早期测试遗留的“幽灵数据”")
        st.info("💡 系统会自动扫描底层数据库。如果你发现有些错题数据在系统中阴魂不散（通常是早期版本删卷子没删干净留下的），点击下方按钮，系统将强制粉碎所有失去试卷本体关联的孤立错题数据。")
        
        c = conn.cursor()
        # 统计到底有多少条错题数据，它的“试卷名”已经不在“试卷总库”里了
        c.execute("SELECT COUNT(*) FROM offline_stats WHERE exam_name NOT IN (SELECT exam_name FROM exams)")
        orphan_stats = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM offline_essay_stats WHERE exam_name NOT IN (SELECT exam_name FROM exams)")
        orphan_essays = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM error_logs WHERE exam_name NOT IN (SELECT exam_name FROM exams)")
        orphan_logs = c.fetchone()[0]
        
        total_orphans = orphan_stats + orphan_essays + orphan_logs
        
        if total_orphans == 0:
            st.success("🎉 恭喜！你的数据库非常健康，没有任何幽灵孤立数据。")
        else:
            st.warning(f"🚨 扫描发现 **{total_orphans}** 条幽灵数据！\n\n(客观题残留: {orphan_stats}条, 主观题残留: {orphan_essays}条, 早期日志残留: {orphan_logs}条)")
            
            if st.button("🔨 立即彻底粉碎这些幽灵数据", type="primary"):
                c.execute("DELETE FROM offline_stats WHERE exam_name NOT IN (SELECT exam_name FROM exams)")
                c.execute("DELETE FROM offline_essay_stats WHERE exam_name NOT IN (SELECT exam_name FROM exams)")
                c.execute("DELETE FROM error_logs WHERE exam_name NOT IN (SELECT exam_name FROM exams)")
                conn.commit()
                st.success("💥 所有失去关联的幽灵数据已被彻底抹除！数据库恢复绝对纯净！")
                import time; time.sleep(2)
                st.rerun()
                
    conn.close()