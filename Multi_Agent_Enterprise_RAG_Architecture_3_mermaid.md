Multi-Agent Enterprise RAG Architecture

Kafka + Spark + Docling/OCR + Vector DB + LangGraph/ADK + Gemini/Vertex AI

This architecture is designed for enterprise-scale document analysis over huge document volumes: PDFs, scanned PDFs, DOCX, PPTX, TXT, images, contracts, RFPs, finance documents, HR policies, technical manuals, and mixed-layout documents.

The design is multi-agent, but not “LLM agent for everything.” The correct approach is:

Deterministic distributed systems for heavy processing+Specialized agents for orchestration and decisions+LLMs only for reasoning, hard enrichment, verification, and final answers

ADK is suitable for production-grade agent systems because its documentation describes it as an open-source framework for building, debugging, and deploying enterprise agents, with support for graph workflows, multi-agent workflows, model routing, Gemini, local models, observability, evaluation, and deployment to Cloud Run/GKE/Agent Runtime. LangChain gives the model/tool harness, LangGraph gives advanced orchestration with durable execution and human-in-the-loop support, and LangSmith provides tracing, debugging, and evaluation.

# 1. Whole Project Architecture

```mermaid
flowchart TB
  ES["Enterprise Sources<br/>SharePoint, S3, Blob, GCS,<br/>FTP, Email, APIs, DB"] --> K["Kafka Event Bus"]
  K --> ING["Ingestion Agent Group"]
  ING --> RAW["Raw Object Storage<br/>/raw/tenant/doc_id/version/file"]
  RAW --> CLS["Spark Classification Cluster"]
  CLS --> DIP["Document Intelligence Profile"]
  DIP --> ROUTER["Parser Router Agent"]
  ROUTER --> SINGLE["Single Parser Path"]
  ROUTER --> ENSEMBLE["Multi-Parser Ensemble Path"]
  ROUTER --> MANUAL["Manual Review / Dead Letter"]
  SINGLE --> PARSER["Parser Agent Group<br/>Docling, Unstructured,<br/>pdfplumber, OCR"]
  ENSEMBLE --> PARSER
  PARSER --> JUDGE["Parser Quality Judge Agent"]
  JUDGE --> MD["Canonical Markdown<br/>Builder Agent"]
  MD --> JSON["Structured JSON Builder<br/>Agent"]
  MD --> MDS[("Processed Markdown Store")]
  JSON --> JSONS[("Processed JSON Store")]
  MDS --> CHUNK["Chunking Agent Group"]
  JSONS --> CHUNK
  CHUNK --> CSTORE["Chunk Store + Metadata DB"]
  CSTORE --> EMB["Spark Embedding Agent<br/>Group"]
  EMB --> VW["Vector Writer Agent"]
  VW --> VDB[("Vector DB<br/>Milvus/Qdrant/Weaviate/OpenSearch")]
  VW --> BM25[("Keyword Index<br/>OpenSearch/BM25")]
  VW --> META[("Metadata DB<br/>PostgreSQL")]
  USER["User Question"] --> ORCH["Query-Time Multi-Agent<br/>Orchestrator<br/>LangGraph / ADK"]
  ORCH --> ACL["Security + ACL Agent"]
  ACL --> QP["Query Planner Agent"]
  QP --> DRA["Domain Retrieval Agents"]
  DRA --> VDB
  DRA --> BM25
  DRA --> META
  DRA --> FUSE["Fusion + Dedup Agent"]
  FUSE --> RERANK["Reranker Agent"]
  RERANK --> VERIFY["Evidence Verifier Agent"]
  VERIFY --> ANSWER["Answer Synthesis Agent<br/>Gemini / Open Model / Vertex AI"]
  ANSWER --> FINAL["Cited Answer or Insufficient Evidence"]
```

Figure 1. Complete Multi-Agent RAG System

# 2. Main Design Layers

| Layer | Purpose | Recommended Tools |
| --- | --- | --- |
| Event backbone | Move document events between services | Kafka |
| Distributed compute | High-speed batch/stream processing | Spark, Spark Structured Streaming |
| Document intelligence | Parse PDFs, DOCX, PPTX, OCR, tables, layout | Docling, Unstructured, pdfplumber, Tesseract, PaddleOCR |
| Agent orchestration | Multi-agent workflows and state transitions | LangGraph, Google ADK |
| Model/tool abstraction | LLM, tool, retriever interfaces | LangChain |
| Observability | Trace every agent/tool/model call | LangSmith, OpenTelemetry, Prometheus, Grafana |
| Storage | Raw files, markdown, JSON, metadata | Object storage, PostgreSQL, Iceberg/Delta |
| Search | Semantic + keyword retrieval | Milvus/Qdrant/Weaviate/OpenSearch |
| LLM reasoning | Query planning, enrichment, verification, answer | Gemini/Vertex AI, vLLM-hosted open models |

Spark is useful here because Spark’s Kafka integration supports parallelism, Kafka partition correspondence, and offset metadata access, which fits document-event processing. Docling is a strong parser base because it supports multiple formats, advanced PDF understanding, layout, reading order, table structure, OCR, Markdown/JSON export, and integrations with LangChain/LlamaIndex/CrewAI/Haystack/vector databases. Unstructured is useful as a complementary parser because it supports partitioning, cleaning, extracting, staging, chunking, embedding workflows, and document elements/metadata; however, its open-source library is better for prototyping than production unless you build the required scaling and governance around it.

# 3. Agent Catalog

| Agent Group | Main Responsibility |
| --- | --- |
| Ingestion Agent Group | Detect files, validate, checksum, version, store raw file, publish event |
| Classification Agent Group | Identify file type, OCR need, layout complexity, document category, risk |
| Parser Router Agent | Decide single parser, ensemble parser, OCR path, layout path, or manual review |
| Parser Agent Group | Run Docling, OCR, pdfplumber, Unstructured, layout-aware extraction |
| Parser Quality Judge Agent | Score parser outputs and decide best/merged result |
| Markdown Builder Agent | Create canonical LLM-ready markdown |
| JSON Builder Agent | Store machine-readable page/layout/table/image structure |
| Chunking Agent Group | Create structure-aware parent/child chunks |
| Embedding Agent Group | Choose embedding model and generate vectors at scale |
| Vector Writer Agent | Idempotently write vectors, metadata, ACLs, versions |
| Query Planner Agent | Convert user question into retrieval plan |
| Domain Retrieval Agents | Search contracts, RFP, finance, policies, technical docs, etc. |
| Fusion + Dedup Agent | Merge keyword/vector/domain results |
| Reranker Agent | Select best evidence chunks |
| Evidence Verifier Agent | Check claims, citations, ACLs, latest versions |
| Answer Synthesis Agent | Generate final answer from verified evidence |
| Audit/Observability Agent | Track lineage, traces, latency, cost, quality |

# 4. Ingestion Agent Group

## Purpose

This agent group receives file discovery events and prepares reliable raw-document records.

It should not parse the file yet. It only answers:

- Is this file valid?

- Is it new or duplicate?

- Where is it stored?

- Who can access it?

- What event should happen next?

```mermaid
flowchart LR
  SRC["Source Event<br/>SharePoint/S3/Blob/API"] --> WATCH["Source Watcher Agent"]
  WATCH --> VALID["File Validation Agent"]
  VALID --> CHECK["Checksum Agent"]
  VALID --> DL["Dead Letter<br/>invalid/corrupt/unsupported"]
  CHECK --> VERSION["Version Detection Agent"]
  VERSION --> SEC["Security Metadata Agent"]
  SEC --> STORE["Raw Storage Agent"]
  STORE --> RAW[("Raw Object Store")]
  STORE --> K[("Kafka:<br/>document.raw.stored")]
```

Figure 2. Ingestion Agent Architecture

## Workflow

# 1. Receive document.discovered event.

# 2. Validate extension, MIME type, file size, checksum.

# 3. Detect duplicate or new version.

# 4. Extract source metadata and ACL.

# 5. Store original file in raw object storage.

# 6. Publish document.raw.stored.

# 7. Send corrupted/encrypted/unsupported files to dead-letter/manual-review queue.

## Output Event

{ "event_type": "document.raw.stored", "document_id": "doc_789", "tenant_id": "company_a", "version": 4, "raw_file_uri": "s3://rag/raw/company_a/doc_789/v4/original.pdf", "checksum": "sha256:abc123", "security_acl": ["legal_team", "finance_team"]}

Kafka should carry metadata and object-storage pointers, not large file bytes. Kafka’s official documentation exposes key concepts, APIs, Connect, Streams, operations, and security as first-class areas, which supports this event-bus role.

# 5. Document Classification Agent Group

## Purpose

This is the first intelligence layer. It decides the document complexity before parsing.

It generates a Document Intelligence Profile.

```mermaid
flowchart TB
  IN[("Kafka:<br/>document.raw.stored")] --> SPARK["Spark Classification Job"]
  SPARK --> FT["File Type Agent"]
  SPARK --> TXT["Text Layer Agent"]
  SPARK --> OCR["OCR Need Agent"]
  SPARK --> LAYOUT["Layout Complexity Agent"]
  SPARK --> TABLE["Table Density Agent"]
  SPARK --> IMAGE["Image Density Agent"]
  SPARK --> LANG["Language Agent"]
  SPARK --> CAT["Document Category Agent"]
  SPARK --> RISK["Risk / Sensitivity Agent"]
  FT --> DIP["Document Intelligence<br/>Profile"]
  TXT --> DIP
  OCR --> DIP
  LAYOUT --> DIP
  TABLE --> DIP
  IMAGE --> DIP
  LANG --> DIP
  CAT --> DIP
  RISK --> DIP
  DIP --> OUT[("Kafka:<br/>document.classification.completed")]
```

Figure 3. Classification Agent Architecture

## Workflow

# 1. Spark consumes document.raw.stored events.

# 2. Read file metadata and lightweight document signals.

# 3. Detect file type: PDF, DOCX, PPTX, TXT, image, email.

# 4. Detect digital text layer vs scanned content.

# 5. Estimate OCR need.

# 6. Estimate layout/table/image complexity.

# 7. Classify domain: contract, RFP, finance, HR, policy, technical.

# 8. Detect sensitivity: PII, PHI, PCI, confidential, legal hold.

# 9. Generate Document Intelligence Profile.

## Document Intelligence Profile

{ "document_id": "doc_789", "file_type": "pdf", "document_category": "contract", "text_density_score": 0.91, "ocr_required_score": 0.05, "table_density_score": 0.72, "image_density_score": 0.34, "layout_complexity_score": 0.68, "classification_confidence": 0.86, "recommended_path": "docling_plus_pdfplumber"}

# 6. Parser Router Agent

## Purpose

The Parser Router Agent chooses the right extraction path.

This is where your system becomes smarter than normal RAG.

```mermaid
flowchart TB
  DIP["Document Intelligence<br/>Profile"] --> ROUTER["Parser Router Agent"]
  ROUTER --> C1{"Confidence >= 0.85?"}
  C1 -->|Yes| SINGLE["Single Parser Path"]
  C1 -->|No| C2{"Confidence 0.60 - 0.85?"}
  C2 -->|Yes| VALIDATOR["Primary Parser + Validator Path"]
  C2 -->|No| ENSEMBLE["Multi-Parser Ensemble Path"]
  ROUTER --> O{"OCR Score High?"}
  O -->|Yes| OCR["Force OCR Path"]
  ROUTER --> T{"Table Density High?"}
  T -->|Yes| TABLE["Add Table Extraction Agent"]
  ROUTER --> L{"Image/Layout Complex?"}
  L -->|Yes| VISION["Add Layout Vision Agent"]
  ROUTER --> BAD{"Corrupt/Encrypted?"}
  BAD -->|Yes| DL["Manual Review / Dead Letter"]
```

Figure 4. Parser Router Agent Architecture

## Routing Rules

classification_confidence >= 0.85 -> single parser0.60 <= classification_confidence < 0.85 -> primary parser + validator parserclassification_confidence < 0.60 -> multi-parser ensembleocr_required_score >= 0.70 -> OCR-first pathtable_density_score >= 0.60 -> add table extraction agentimage_density_score >= 0.60 or layout_complexity_score >= 0.70 -> add layout/image caption agentcorrupt/encrypted/password-protected -> manual review or dead letter

# 7. Parser Agent Group

## Purpose

This group extracts text, tables, images, layout, page numbers, bounding boxes, and metadata.

```mermaid
flowchart TB
  DEC["Parser Router Decision"] --> GROUP["Parser Agent Group"]
  GROUP --> DOCLING["Docling Agent<br/>layout, reading order,<br/>tables, markdown/json"]
  GROUP --> UNSTRUCT["Unstructured Agent<br/>elements, metadata,<br/>partitioning"]
  GROUP --> PDF["pdfplumber Agent<br/>PDF text/tables"]
  GROUP --> OCR["OCR Agent<br/>Tesseract/PaddleOCR/Surya OCR"]
  GROUP --> OFFICE["Office Agent<br/>DOCX/PPTX extraction"]
  GROUP --> LAYOUT["Layout Vision Agent<br/>image/text alignment"]
  GROUP --> LLM["LLM Fragment Agent<br/>only unclear fragments"]
  DOCLING --> STORE["Parser Output Store"]
  UNSTRUCT --> STORE
  PDF --> STORE
  OCR --> STORE
  OFFICE --> STORE
  LAYOUT --> STORE
  LLM --> STORE
```

Figure 5. Parser Agent Group Architecture

## Tool Responsibility

| Agent | Best For |
| --- | --- |
| Docling Agent | Main document parser, layout, reading order, tables, Markdown/JSON |
| Unstructured Agent | Element extraction, quick parsing, alternate validation |
| pdfplumber Agent | PDF tables and exact text-region extraction |
| OCR Agent | Scanned pages, image text |
| Office Agent | DOCX/PPTX structure, slides, comments, notes |
| Layout Vision Agent | Images, diagrams, chart-text alignment |
| LLM Fragment Agent | Only hard fragments, not entire documents |

Docling’s feature list includes parsing for PDF/DOCX/PPTX/XLSX/HTML/images/plain text and advanced PDF understanding such as page layout, reading order, table structure, code, formulas, image classification, Markdown export, JSON export, OCR, VLM support, and local execution.

# 8. Multi-Parser Ensemble Agent

## Purpose

For difficult documents, do not trust one parser. Run multiple parsers, compare results, and merge the best pieces.

```mermaid
flowchart TB
  DOC["Complex / Low Confidence<br/>Document"] --> ENS["Multi-Parser Ensemble<br/>Agent"]
  ENS --> D["Docling Output"]
  ENS --> U["Unstructured Output"]
  ENS --> P["pdfplumber Table Output"]
  ENS --> O["OCR Output"]
  ENS --> V["Layout Vision Output"]
  ENS --> L["LLM Fragment Output"]
  D --> J["Parser Quality Judge"]
  U --> J
  P --> J
  O --> J
  V --> J
  L --> J
  J --> BT["Best Text Source"]
  J --> BTA["Best Table Source"]
  J --> BO["Best OCR Source"]
  J --> BI["Best Image Caption Source"]
  BT --> MD["Canonical Markdown Builder"]
  BTA --> MD
  BO --> MD
  BI --> MD
```

Figure 6. Multi-Parser Ensemble Architecture

## Quality Score

final_parse_score = 0.20 * text_coverage_score + 0.15 * reading_order_score + 0.15 * table_quality_score + 0.15 * layout_preservation_score + 0.10 * OCR_confidence_score + 0.10 * image_caption_coverage_score + 0.10 * metadata_completeness_score - 0.05 * duplicate_noise_score

## Important Rule

Do not run this expensive path for every document.

Simple document -> one parserMedium document -> primary parser + validatorComplex document -> parser ensembleCritical document -> parser ensemble + human review if confidence is low

# 9. Parser Quality Judge Agent

## Purpose

The quality judge decides whether the parser output is trustworthy.

```mermaid
flowchart LR
  OUT["Parser Outputs"] --> TC["Text Coverage Check"]
  OUT --> RO["Reading Order Check"]
  OUT --> TQ["Table Quality Check"]
  OUT --> OC["OCR Confidence Check"]
  OUT --> LP["Layout Preservation Check"]
  OUT --> ND["Noise/Duplicate Check"]
  TC --> SCORE["Parse Confidence Score"]
  RO --> SCORE
  TQ --> SCORE
  OC --> SCORE
  LP --> SCORE
  ND --> SCORE
  SCORE --> TH{"Score >= Threshold?"}
  TH -->|Yes| APPROVE["Approve for Markdown"]
  TH -->|No| FALLBACK["Fallback Parser / LLM<br/>Fragment / Human Review"]
```

Figure 7. Parser Quality Judge Workflow

Output

{ "document_id": "doc_789", "best_text_source": "docling", "best_table_source": "pdfplumber", "best_ocr_source": "paddleocr", "parse_confidence": 0.92, "manual_review_required": false}

# 10. Canonical Markdown Builder Agent

## Purpose

This agent creates the final LLM-friendly document.

The markdown file becomes the trusted “clean document” used for chunking, audit, and citations.

```mermaid
flowchart TB
  BEST["Best Parser Outputs"] --> MB["Markdown Builder Agent"]
  MB --> H["Normalize Headings"]
  MB --> A["Preserve Page Anchors"]
  MB --> T["Preserve Tables"]
  MB --> I["Insert Image Captions"]
  MB --> R["Remove Header/Footer Noise"]
  MB --> M["Attach Metadata"]
  MB --> C["Attach Confidence Scores"]
  H --> CAN["Canonical Markdown"]
  A --> CAN
  T --> CAN
  I --> CAN
  R --> CAN
  M --> CAN
  C --> CAN
  CAN --> STORE[("Processed Markdown Store")]
```

Figure 8. Markdown Builder Architecture

## Example Output

# Vendor ABC Master Services Agreementmetadata: document_id: doc_789 version: 4 document_type: contract parser_path: docling_plus_pdfplumber parse_confidence: 0.92 security_acl: - legal_team - finance_team---## Page 2: Payment TermsInvoices shall be paid within thirty calendar days after approval.### Table: Pricing Schedule| Service | Unit Cost | Billing Frequency ||---|---:|---|| Data Processing | 1200 | Monthly |## Page 5: Approval Workflow DiagramImage description:The diagram shows vendor invoice submission, legal approval, finance approval,and payment execution.

# 11. Structured JSON Builder Agent

## Purpose

Markdown is good for humans and LLMs. JSON is good for systems.

The JSON stores page coordinates, tables, image locations, confidence, and lineage.

```mermaid
flowchart TB
  PM["Parser Outputs + Markdown"] --> JB["Structured JSON Builder<br/>Agent"]
  JB --> PS["Page Structure"]
  JB --> SH["Section Hierarchy"]
  JB --> PE["Paragraph Elements"]
  JB --> TO["Table Objects"]
  JB --> IO["Image Objects"]
  JB --> BB["Bounding Boxes"]
  JB --> OCR["OCR Confidence"]
  JB --> PL["Parser Lineage"]
  PS --> DOC["Processed JSON Document"]
  SH --> DOC
  PE --> DOC
  TO --> DOC
  IO --> DOC
  BB --> DOC
  OCR --> DOC
  PL --> DOC
  DOC --> STORE[("Processed JSON Store")]
```

Figure 9. Structured JSON Builder Architecture

## JSON Example

{ "document_id": "doc_789", "version": 4, "sections": [ { "title": "Payment Terms", "page_start": 2, "page_end": 2, "elements": [ { "type": "paragraph", "text": "Invoices shall be paid within thirty calendar days after approval.", "bbox": [100, 250, 500, 290], "confidence": 0.96 } ] } ]}

# 12. Chunking Agent Group

## Purpose

Chunking should depend on document type.

A contract should not be chunked the same way as a PPTX or finance spreadsheet.

```mermaid
flowchart TB
  IN["Canonical Markdown +<br/>Structured JSON"] --> ROUTER["Chunking Router Agent"]
  ROUTER --> CONTRACT["Contract Chunking Agent<br/>clauses, obligations"]
  ROUTER --> RFP["RFP Chunking Agent<br/>requirements, Q/A"]
  ROUTER --> POLICY["Policy Chunking Agent<br/>sections, subsections"]
  ROUTER --> FIN["Finance Chunking Agent<br/>tables, approvals, notes"]
  ROUTER --> PPT["PPTX Chunking Agent<br/>slides, notes, images"]
  ROUTER --> TECH["Technical Chunking Agent<br/>procedures, APIs, steps"]
  ROUTER --> IMG["Image/Table Chunking<br/>Agent"]
  CONTRACT --> PARENT["Parent-Child Chunk Agent"]
  RFP --> PARENT
  POLICY --> PARENT
  FIN --> PARENT
  PPT --> PARENT
  TECH --> PARENT
  IMG --> PARENT
  PARENT --> QA["Chunk QA Agent"]
  QA --> STORE["Chunk Store"]
```

Figure 10. Chunking Agent Group Architecture

## Chunking Rules

| Document Type | Chunking Strategy |
| --- | --- |
| Contract | Clause + section parent chunks |
| RFP | Requirement/question/answer chunks |
| Policy | Heading/subheading chunks |
| PPTX | Slide parent + bullet/image child chunks |
| Finance | Table chunks + approval notes |
| Technical docs | Procedure/API/step chunks |
| Scanned PDF | Page/section chunks with OCR confidence |
| Image-heavy document | Image caption + nearby text chunk |

## Chunk Object

{ "chunk_id": "doc_789_v4_chunk_021", "document_id": "doc_789", "version": 4, "chunk_type": "contract_clause", "section_path": "Payment Terms > Invoice Approval", "page_start": 2, "page_end": 2, "text": "Invoices shall be paid within thirty calendar days after approval.", "parent_chunk_id": "doc_789_v4_section_payment_terms", "security_acl": ["legal_team", "finance_team"], "source_markdown_uri": "s3://processed_markdown/company_a/doc_789/v4/document.md"}

# 13. Embedding Agent Group

## Purpose

This group generates embeddings at scale using Spark.

It should not embed bad chunks, empty chunks, unauthorized chunks, or old versions incorrectly.

```mermaid
flowchart TB
  K[("Kafka:<br/>document.embedding.requested")] --> SPARK["Spark Embedding Job"]
  SPARK --> ROUTER["Embedding Model Router<br/>Agent"]
  ROUTER --> TEXT["Text Embedding Agent"]
  ROUTER --> TABLE["Table Embedding Agent"]
  ROUTER --> IMG["Image Caption Embedding<br/>Agent"]
  ROUTER --> MULTI["Multilingual Embedding<br/>Agent"]
  ROUTER --> TECH["Technical/Code Embedding<br/>Agent"]
  TEXT --> VAL["Vector Validation Agent"]
  TABLE --> VAL
  IMG --> VAL
  MULTI --> VAL
  TECH --> VAL
  VAL --> OK{"Valid Vector?"}
  OK -->|Yes| WRITER["Vector Writer Agent"]
  OK -->|No| DL["Embedding Dead Letter /<br/>Rechunk"]
  WRITER --> VDB[("Vector DB")]
  WRITER --> META[("Metadata DB")]
  WRITER --> KEY[("Keyword Index")]
```

Figure 11. Embedding Agent Architecture

## Embedding Model Choices

| Chunk Type | Model Choice |
| --- | --- |
| Normal text | ModernBERT, BGE, E5, GTE |
| Long technical text | ModernBERT / long-context embedding |
| Multilingual text | BGE-M3, multilingual-E5, Gemini embeddings |
| Tables | table-to-text + table metadata embedding |
| Images | caption embedding, not raw image by default |
| Code | code-aware embedding |
| Low-confidence OCR | skip, review, or embed with confidence penalty |

# 14. Vector Writer Agent

## Purpose

The Vector Writer Agent writes vectors safely.

```mermaid
flowchart TB
  IN["Validated Embeddings"] --> VW["Vector Writer Agent"]
  VW --> DIM["Check Vector Dimension"]
  VW --> META["Check Metadata<br/>Completeness"]
  VW --> ACL["Check ACL Exists"]
  VW --> VER["Check Version / Latest Flag"]
  VW --> ID["Build Vector ID"]
  VW --> UPSERT["Idempotent Upsert"]
  VW --> KEY["Write Keyword Index"]
  VW --> MDB["Write Metadata DB"]
  VW --> EVENT["Emit index.ready Event"]
  UPSERT --> VDB[("Vector DB<br/>Collection/Partition")]
  KEY --> BM25[("OpenSearch / BM25")]
  MDB --> PG[("PostgreSQL Metadata DB")]
```

Figure 12. Vector Writer Architecture

Milvus is a strong enterprise vector DB option because its documentation describes open-source and distributed deployment modes, Kubernetes-based distributed deployment for billion-scale or larger scenarios, ANN search, filtering search, hybrid search, full-text BM25 search, and reranking.

# 15. Query-Time Multi-Agent Orchestration

## Purpose

This is the live user-question flow.

Use LangGraph or ADK here, not Spark.

Spark is for heavy background processing. Query-time retrieval needs low-latency orchestration.

```mermaid
flowchart TB
  U["User Question"] --> C["Conversation Agent"]
  C --> ACL["Security + ACL Agent"]
  ACL --> QU["Query Understanding Agent"]
  QU --> PLAN["Retrieval Planner Agent"]
  PLAN --> D{"Single Domain or Multi<br/>Domain?"}
  D -->|Single| SINGLE["Single Domain Retrieval<br/>Agent"]
  D -->|Multi| MORCH["Multi-Domain Retrieval<br/>Orchestrator"]
  MORCH --> CONTRACT["Contract Retrieval Agent"]
  MORCH --> RFP["RFP Retrieval Agent"]
  MORCH --> FIN["Finance Retrieval Agent"]
  MORCH --> POL["Policy Retrieval Agent"]
  MORCH --> TECH["Technical Docs Retrieval<br/>Agent"]
  SINGLE --> FUSE["Fusion + Dedup Agent"]
  CONTRACT --> FUSE
  RFP --> FUSE
  FIN --> FUSE
  POL --> FUSE
  TECH --> FUSE
  FUSE --> RERANK["Reranker Agent"]
  RERANK --> VERIFY["Evidence Verifier Agent"]
  VERIFY --> ENOUGH{"Enough Verified Evidence?"}
  ENOUGH -->|Yes| ANSWER["Answer Synthesis Agent"]
  ENOUGH -->|No| ASK["Abstain / Ask Clarifying<br/>Question"]
  ANSWER --> AUDIT["Citation + Audit Agent"]
  ASK --> AUDIT
  AUDIT --> FINAL["Final Response"]
```

Figure 13. Query-Time Multi-Agent Architecture

# 16. Query Planner Agent

## Purpose

The Query Planner Agent converts the user’s question into a retrieval plan.

This is like Spark Catalyst, but for RAG retrieval.

```mermaid
flowchart TB
  U["User Question"] --> INTENT["Intent Detection"]
  INTENT --> ENTITY["Entity Extraction"]
  ENTITY --> DOMAIN["Domain Detection"]
  DOMAIN --> FILTER["Metadata Filter Builder"]
  FILTER --> ACL["ACL Resolver"]
  ACL --> COLLECTION["Collection Selection"]
  COLLECTION --> TOPK["Top-K Allocation"]
  TOPK --> STRAT["Retrieval Strategy Selection"]
  STRAT --> PLAN["Physical Retrieval Plan"]
```

Figure 14. Query Planner Workflow

## Example Question

Compare Vendor ABC's RFP pricing requirement, signed contract payment terms,and finance approval notes.

## Logical Plan

{ "intent": "compare_documents", "domains": ["rfp", "contract", "finance"], "entities": { "vendor": "Vendor ABC" }, "required_evidence": [ "pricing requirement", "payment terms", "approval notes" ]}

## Physical Plan

{ "collections": [ "rfp_vectors", "contracts_vectors", "finance_vectors" ], "filters": { "tenant_id": "company_a", "vendor": "Vendor ABC", "latest_version": true, "security_acl": "resolved_user_acl" }, "retrieval_strategy": "hybrid", "top_k_per_collection": 50, "reranker": "cross_encoder_or_gemini_judge"}

# 17. Domain Retrieval Agents

## Purpose

Each domain retrieval agent understands one business domain.

```mermaid
flowchart TB
  PLAN["Retrieval Plan"] --> DRA["Domain Retrieval Agent"]
  DRA --> ACL["Apply ACL Filter"]
  DRA --> META["Apply Metadata Filters"]
  DRA --> VEC["Vector Search"]
  DRA --> KEY["Keyword/BM25 Search"]
  DRA --> EXACT["Exact ID Search"]
  DRA --> VER["Version Filter"]
  ACL --> CAND["Candidate Results"]
  META --> CAND
  VEC --> CAND
  KEY --> CAND
  EXACT --> CAND
  VER --> CAND
  CAND --> RANK["Domain-Level Ranked<br/>Results"]
```

Figure 15. Domain Retrieval Agent Pattern

## Domain Agents

| Agent | Search Focus |
| --- | --- |
| Contract Retrieval Agent | Clauses, obligations, payment terms, signed version |
| RFP Retrieval Agent | Requirements, proposals, scoring, pricing |
| Finance Retrieval Agent | Approval notes, invoices, budgets, tables |
| HR Policy Retrieval Agent | HR rules, benefits, PTO, compliance |
| Legal Retrieval Agent | Legal memos, case references, contracts |
| Technical Docs Agent | APIs, architecture, SOPs, manuals |
| Security/Compliance Agent | Controls, policies, audit evidence |

# 18. Fusion + Dedup Agent

## Purpose

When multiple agents return results, this agent merges them.

```mermaid
flowchart LR
  V["Vector Results"] --> F["Fusion + Dedup Agent"]
  K["Keyword Results"] --> F
  M["Metadata/Exact Match<br/>Results"] --> F
  F --> N["Normalize Scores"]
  N --> RRF["Reciprocal Rank Fusion"]
  RRF --> D["Deduplicate Chunks"]
  D --> S["Ensure Source Diversity"]
  S --> C["Candidate Evidence Set"]
```

Figure 16. Fusion Agent Architecture

## Fusion Rules

# 1. Prefer chunks that match both semantic and keyword search.

# 2. Prefer exact entity matches.

# 3. Prefer latest version unless user asks historical.

# 4. Remove duplicate chunks from same page/section.

# 5. Keep source diversity across contracts, RFPs, finance docs.

# 6. Never include unauthorized chunks.

# 19. Reranker Agent

## Purpose

The Reranker Agent reduces many candidates to the best evidence.

```mermaid
flowchart TB
  CAND["Candidate Evidence Set<br/>Top 50-150 chunks"] --> R["Reranker Agent"]
  R --> SEM["Semantic Relevance"]
  R --> ENT["Entity Match"]
  R --> AUTH["Document Authority"]
  R --> LATEST["Latest Version"]
  R --> CONF["Source/Page Confidence"]
  R --> COVER["Question Coverage"]
  SEM --> GLOBAL["Global Reranked Evidence"]
  ENT --> GLOBAL
  AUTH --> GLOBAL
  LATEST --> GLOBAL
  CONF --> GLOBAL
  COVER --> GLOBAL
  GLOBAL --> TOP["Top 5-12 Chunks"]
```

Figure 17. Reranker Agent Workflow

## Reranking Criteria

semantic relevanceexact vendor/document/entity matchlatest versiondocument authoritypage/section confidenceparse confidenceOCR confidencequestion coveragesource diversity

# 20. Evidence Verifier Agent

## Purpose

This is the trust layer.

The verifier checks whether the answer can be safely generated.

```mermaid
flowchart TB
  TOP["Top Evidence Chunks"] --> EV["Evidence Verifier Agent"]
  EV --> ACL["ACL Check"]
  EV --> CIT["Citation Exists Check"]
  EV --> LATEST["Latest Version Check"]
  EV --> OCR["OCR/Parse Confidence<br/>Check"]
  EV --> CONTRA["Contradiction Check"]
  EV --> CLAIM["Claim Support Check"]
  ACL --> SAFE{"Evidence Safe?"}
  CIT --> SAFE
  LATEST --> SAFE
  OCR --> SAFE
  CONTRA --> SAFE
  CLAIM --> SAFE
  SAFE -->|Yes| SEND["Send to Answer Agent"]
  SAFE -->|No| ABSTAIN["Abstain / Ask Clarifying<br/>Question / Human Review"]
```

Figure 18. Evidence Verifier Architecture

## Verification Rules

Every claim must be supported by retrieved evidence.Every citation must map to a real document/page/section.The user must have access to every cited chunk.Old versions must not be used unless requested.Low-confidence OCR should be disclosed or excluded.Conflicting evidence should be surfaced, not hidden.

# 21. Answer Synthesis Agent

## Purpose

This agent writes the final answer using only verified context.

```mermaid
flowchart TB
  V["Verified Evidence"] --> A["Answer Synthesis Agent"]
  A --> P["Build Grounded Prompt"]
  P --> G["Generate Answer"]
  G --> C["Attach Citations"]
  C --> CONF["Confidence Note"]
  CONF --> AUDIT["Audit Log"]
  AUDIT --> FINAL["Final Answer"]
```

Figure 19. Answer Synthesis Agent Workflow

## Output Format

# 1. Answer:Vendor ABC's signed contract states that invoices are payable within thirtycalendar days after approval. The RFP pricing section required monthly billing,and the finance approval memo confirms monthly billing approval.Sources:

# 2. Vendor ABC MSA, Page 2, Payment Terms

# 3. RFP-2026-ABC, Page 14, Pricing Requirements

# 4. Finance Approval Memo, Page 3

Use Gemini/Vertex AI here when you need managed enterprise deployment, strong reasoning, multimodal understanding, and Gemini-native ADK integration. ADK documentation says it provides easy access to Gemini and can connect to other leading and locally running models, with Google Cloud deployment options for performance, reliability, security, access, safety, and cost management.

# 22. Observability and Audit Agent

## Purpose

Every agent call must be traceable.

```mermaid
flowchart TB
  CALLS["All Agent Calls"] --> TRACE["Tracing Middleware"]
  TRACE --> L["LangSmith Traces"]
  TRACE --> O["OpenTelemetry"]
  TRACE --> P["Prometheus Metrics"]
  TRACE --> G["Grafana Dashboards"]
  TRACE --> A["Audit DB"]
  L --> DEBUG["Debug Retrieval Failures"]
  O --> DIST["Distributed Trace"]
  P --> LAT["Latency/Throughput<br/>Metrics"]
  G --> OPS["Ops Dashboards"]
  A --> COMP["Compliance Audit"]
```

Figure 20. Observability Architecture

## Track:

agent nameinput/outputtool callsmodel usedlatencycostretrieved chunksreranked chunksverification resultcitationsuser feedbackfailure reason

LangSmith provides visibility from individual traces to production-wide metrics, supports framework/provider integrations, trace filtering/export, dashboards, alerts, automations, and feedback collection.

# 23. Storage Architecture

```mermaid
flowchart TB
  RAW["Raw Files"] --> ROS[("Raw Object Store")]
  MD["Canonical Markdown"] --> MDS[("Processed Markdown Store")]
  JSON["Structured JSON"] --> JSONS[("Processed JSON Store")]
  CHUNKS["Chunks"] --> CS[("Chunk Store<br/>PostgreSQL/Iceberg/Delta")]
  EMB["Embeddings"] --> VDB[("Vector DB")]
  TEXT["Exact Text"] --> BM25[("OpenSearch BM25")]
  META["Metadata / ACL / Lineage"] --> MDB[("Metadata DB")]
  AUDIT["Traces / Audits"] --> ADB[("Audit/Observability DB")]
```

Figure 21. Storage Architecture

## Store Responsibilities

| Store | Responsibility |
| --- | --- |
| Raw Object Store | Original untouched files |
| Markdown Store | Clean LLM-ready markdown |
| JSON Store | Machine-readable layout/table/page data |
| Chunk Store | Chunk objects before embedding |
| Vector DB | Semantic search |
| OpenSearch | Keyword, exact ID, BM25 search |
| Metadata DB | ACLs, versions, lineage, status |
| Audit DB | Traces, answer history, retrieved evidence |

# 24. Kafka Topic Architecture

```mermaid
flowchart LR
  D["document.discovered"] --> RAW["document.raw.stored"]
  RAW --> C["document.classification.completed"]
  C --> R["document.parser.route.selected"]
  R --> S["document.parsing.started"]
  S --> DONE["document.parsed.completed"]
  DONE --> MD["document.markdown.ready"]
  MD --> CH["document.chunked.completed"]
  CH --> EMB["document.embedding.completed"]
  EMB --> IDX["document.indexed.completed"]
  C -.-> DL["document.deadletter"]
```

Figure 22. Kafka Event Flow

## Kafka Event Rule

Use:

partition_key = tenant_id + document_id

This keeps events for the same document ordered while allowing huge parallelism across different documents.

Spark’s Kafka integration documentation discusses partition-to-partition parallelism, offsets, metadata, and offset storage choices; for production, vector writes and metadata updates should be idempotent and offsets should be committed only after output is safely stored.

# 25. Deployment Architecture

```mermaid
flowchart TB
  subgraph CLOUD["Kubernetes / Cloud Platform"]
    K["Kafka Cluster"] --> S["Spark Cluster"]
    S --> P["Parser Workers"]
    S --> E["Embedding Workers"]
    API["API Gateway"] --> Q["Query Orchestrator"]
    Q --> AG["Agent Services<br/>LangGraph/ADK"]
  end
  subgraph STORAGE["Storage"]
    OBJ[("Object Storage")]
    VDB[("Vector DB Cluster")]
    OS[("OpenSearch Cluster")]
    PG[("PostgreSQL Metadata DB")]
  end
  subgraph OBS["Observability"]
    LS["LangSmith"]
    OT["OpenTelemetry Collector"] --> PROM["Prometheus"] --> GRAF["Grafana"]
  end
  P --> OBJ
  E --> VDB
  E --> OS
  AG --> VDB
  AG --> OS
  AG --> PG
  AG --> LS
  AG --> OT
```

Figure 23. Deployment View

# 26. Final Recommended Agent Boundaries

## Background Processing Agents

These are event-driven and Spark-friendly:

Ingestion Agent GroupClassification Agent GroupParser Router AgentParser Agent GroupParser Quality Judge AgentMarkdown Builder AgentJSON Builder AgentChunking Agent GroupEmbedding Agent GroupVector Writer Agent

## Query-Time Agents

These run inside LangGraph/ADK:

Conversation AgentSecurity + ACL AgentQuery Understanding AgentRetrieval Planner AgentDomain Retrieval AgentsFusion + Dedup AgentReranker AgentEvidence Verifier AgentAnswer Synthesis AgentCitation + Audit Agent

# 27. Final Clean Architecture Summary

Kafka moves document events across the platform.Spark performs distributed classification, parsing orchestration, chunking, embedding, and indexing.Docling / Unstructured / pdfplumber / OCR extract clean structure from messy enterprise documents.Markdown + JSON stores preserve both LLM-ready text and machine-readable document structure.Vector DB + OpenSearch provide semantic and keyword retrieval.LangGraph / ADK orchestrate multi-agent query-time reasoning and retrieval.LangChain provides common model/tool/retriever interfaces.LangSmith / OpenTelemetry provide tracing, debugging, monitoring, and evaluation.Gemini / Vertex AI / local models handle final answers, multimodal understanding, hard fragment enrichment, and verification.

The simplest manager-level explanation:

We are building a distributed multi-agent document intelligence platform, not a basic RAG chatbot. Kafka moves document events, Spark processes documents at scale, parser agents create trusted markdown and JSON, embedding agents index the knowledge, retrieval agents search the right domains, verifier agents check evidence, and the answer agent only responds from verified, permission-safe context.
