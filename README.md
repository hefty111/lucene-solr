###### Copyright Notice
<p>
IVrixDB ©Ivri Faitelson 2020<br>
IVrixDB was developed by Ivri Faitelson and is a proprietary software product owned by Ivri Faitelson.<br>
All rights reserved.
</p>


# IVrixDB
IVrixDB is a schema-less time-series search application designed for scale.
It is built upon Solr, an enterprise search platform written on top of Lucene,
which is a high-performance, full-featured text search engine library.
IVrixDB's main feature is Search-Time Field Extraction, where it can run search and analytics
on Solr Fields that do not exist in the schema.

IVrixDB's goal is to provide the following:
- schema-less time-series indexes
- search-time field extraction
- interactive and incremental search
- full-text search and analytics
- time-based data retention
- SolrCloud/cluster/scale-out support
- hardware resource management
- fault-tolerant architecture

IVrixDB is at the stage of an advanced proof-of-concept, and all the
features above have been successfully implemented with scale in mind.

Despite being embedded within Solr (for development ease), IVrixDB is an extension of Solr.
It is built upon Solr’s extensible plug-in architecture. Solr's existing functionality was not impacted,
and there is little variation in performance or scalability when compared to Solr.

The code can be located at package [org.apache.solr.ivrixdb](solr/core/src/java/org/apache/solr/ivrixdb/),
and the resources containing IVrixDB documents, tools, configurations, and more can be found in the [IVrixDB Resources](IVrixDB%20Resources/) folder.

To further understand IVrixDB's features, please read the [Key Features In-Depth Breakdown](IVrixDB%20Resources/Key%20Features%20In-Depth%20Breakdown.md) document.

To further understand the current architecture and its guiding principles, please read the [IVrixDB Architecture](IVrixDB%20Resources/documentation/IVrixDB%20Architecture.md) document.

To know what was not fully implemented and what are the current issues in the project,
please read the [Disclaimers, Warnings, and Bugs](IVrixDB%20Resources/Disclaimers,%20Warnings,%20and%20Bugs.md) document.

To start using this application, please read the [Quick Start](IVrixDB%20Resources/Quick%20Start.md) document.

To begin development, please read the documents in the order presented:
[IVrixDB Architecture](IVrixDB%20Resources/documentation/IVrixDB%20Architecture.md), [Disclaimers, Warnings, and Bugs](IVrixDB%20Resources/Disclaimers,%20Warnings,%20and%20Bugs.md), [Quick Start](IVrixDB%20Resources/Quick%20Start.md), and [IVrixDB Tests](IVrixDB%20Resources/documentation/IVrixDB%20Tests.md).

To contact me, please email ivrift@icloud.com

## P.S.
Apologies in advance for any unclear documentation or code.
College was creeping up on me during the end of my senior year of high-school, and I was feeling rushed.
Despite this, I managed to build a stable and advanced proof-of-concept that can create and search
IVrixDB indexes in the production environment of SolrCloud. I worked hard to keep the code clean and of high-quality,
though some parts of the code do not live up to my standards. Future work will include sharpening both code and documentation.
Please feel free to email me at any time.